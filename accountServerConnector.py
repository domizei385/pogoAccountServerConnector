import asyncio
import datetime
import json
import os
from typing import (Any, Dict, Optional)

import aiohttp
import mapadroid.plugins.pluginBase
from aiohttp import web
from loguru import logger
from mapadroid.account_handler.AbstractAccountHandler import (
    AccountPurpose, BurnType)
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.model import SettingsDevice, SettingsPogoauth
from mapadroid.db.model import TrsStatsDetectWildMonRaw
from mapadroid.utils.madGlobals import InternalStopWorkerException
from mapadroid.utils.collections import Location
from plugins.accountServerConnector.endpoints import register_custom_plugin_endpoints
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select


class accountServerConnector(mapadroid.plugins.pluginBase.Plugin):
    """accountServerConnector plugin
    """

    def _file_path(self) -> str:
        return os.path.dirname(os.path.abspath(__file__))

    async def patch_set_level(self):
        async def new_set_level(device_id: int, level: int):
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                logger.info("Setting level of {} to {}", device_id, level)
                await self._track_level(device_entry.name, level)

        self._account_handler.set_level = new_set_level
        self.logger.success("patched set_level")

    async def patch_get_account(self):
        # TODO: forward purpose and location_to_use
        async def new_get_account(device_id: int, purpose: AccountPurpose,
            location_to_scan: Optional[Location],
            including_google: bool = True) -> Optional[SettingsPogoauth]:
            async with self._assignment_lock:
                # First, fetch all pogoauth accounts
                async with self._db_wrapper as session, session:
                    device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                        session, self._db_wrapper.get_instance_id(), device_id)
                    if not device_entry:
                        logger.warning("Device ID {} not found in device table", device_id)
                        return None
                    origin = device_entry.name
                    level_mode = await self.mm.routemanager_of_origin_is_levelmode(origin)
                    data = await self._request_account(origin, level_mode)
                    if not data:
                        return None
                    auth = SettingsPogoauth(username=data["username"], password=data["password"], level=data["level"],
                                            instance_id=self._db_wrapper.get_instance_id(), device_id=device_id, login_type="ptc")
                    logger.info("Returning auth " + str(auth))
                    return auth
            return None
        self._account_handler.get_account = new_get_account
        self.logger.success("patched get_account")

    async def patch_notify_logout(self):
        async def new_notify_logout(device_id: int) -> None:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                logger.info("Saving logout of {}", origin)

                encounters = await self._count_by_worker(session, worker=origin)
                await self._delete_worker_stats(session, origin)
                await self._logout(origin, encounters)
        self._account_handler.notify_logout = new_notify_logout
        self.logger.success("patched notify_logout")

    # only used for MAD triggered BurnTypes
    async def patch_mark_burnt(self):
        async def new_mark_burnt(device_id: int, burn_type: Optional[BurnType]) -> None:
            logger.info("Trying to mark account of {} as burnt by {}", device_id, burn_type)
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None

                origin = device_entry.name
                encounters = await self._count_by_worker(session, worker=origin)
                await self._burn_account(origin, reason=burn_type.value, encounters=encounters)
                await self._delete_worker_stats(session, origin)

        self._account_handler.mark_burnt = new_mark_burnt
        self.logger.success("patched mark_burnt")

    async def patch_get_assigned_username(self):
        async def new_get_assigned_username(device_id: int) -> Optional[str]:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                assignment = await self._get_account_info(origin)
                if not assignment:
                    return None
                return assignment["username"]

        self._account_handler.get_assigned_username = new_get_assigned_username
        self.logger.success("patched get_assigned_username")

    async def patch_set_last_softban_action(self):
        async def new_set_last_softban_action(device_id: int, time_of_action: datetime.datetime,
            location_of_action: Location) -> None:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                # TODO: api call
                logger.warning("set_last_softban_action: To be implemented")
        self._account_handler.set_last_softban_action = new_set_last_softban_action
        self.logger.success("patched set_last_softban_action")

    async def patch_is_burnt(self):
        async def new_is_burnt(device_id: int) -> bool:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return False
                logger.info("Checking whether account assigned to {} was burnt or not", device_entry.name)
                origin = device_entry.name
                assignment = await self._get_account_info(origin)
                if not assignment:
                    logger.warning(f"Unable to check if origin {origin} is_burnt as it currently has no assignment")
                    return False
                #if assignment["last_reason"] == BurnType.MAINTENANCE or assignment["last_reason"] == BurnType.BAN or assignment["last_reason"] == BurnType.SUSPENDED:
                return True if int(assignment["is_burnt"]) else False
            return False
        self._account_handler.is_burnt = new_is_burnt
        self.logger.success("patched is_burnt")

    async def patch_get_strategy(self):
        old_get_strategy = self.strategy_factory.get_strategy

        async def new_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state):
            strategy = await old_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state)

            # intercept switch_user for switch reason
            async def new_switch_user(reason=None):
                # reason oneof ['maintenance', 'limit', 'level', 'teleport']
                origin = worker_state.origin
                self.logger.debug(f"_switch_user(origin={origin}, reason={reason})")
                async with self._db_wrapper as session, session:
                    encounters = await self._count_by_worker(session, worker=origin)
                    ## mostly copied from original "_switch_user"
                    # stop and wait for remaining data to be processed
                    await strategy.stop_pogo()
                    await asyncio.sleep(5)
                    # must burn account before calling clear_game_data, which triggers logout
                    await self._burn_account(origin, reason=reason, encounters=encounters)
                    await self._delete_worker_stats(session, origin)
                    await strategy._clear_game_data()
                    await asyncio.sleep(5)
                    await self.turn_screen_on_and_start_pogo()
                    if not await self._ensure_pogo_topmost():
                        logger.error('Kill Worker...')
                        raise InternalStopWorkerException("Pogo not topmost app during switching of users")
                    logger.info('Switching finished ...')

            if strategy._switch_user != new_switch_user:
                self.logger.debug("patch _switch_user")
                strategy._switch_user = new_switch_user
                self.__worker_strategy[worker_state.origin] = strategy
            elif strategy._switch_user == new_switch_user:
                self.logger.warning("already patched switch_user")

            old_check_ptc_login_ban = strategy._word_to_screen_matching.check_ptc_login_ban

            async def new_check_ptc_login_ban():
                # Suppress login attempt when no account is available
                count = await self._available_accounts(worker_state.origin)
                allow_login = count > 0 and await old_check_ptc_login_ban()
                self.logger.info(f"Accounts available for {worker_state.origin}: {str(count)}. {'Allowing' if allow_login else 'Preventing'} PTC login.")
                return allow_login
            if strategy._word_to_screen_matching.check_ptc_login_ban != new_check_ptc_login_ban:
                self.logger.debug("patch check_ptc_login_ban")
                strategy._word_to_screen_matching.check_ptc_login_ban = new_check_ptc_login_ban
            # else:
            #     self.logger.info(f"not patching for {worker_state.origin}")
            return strategy

        self.strategy_factory.get_strategy = new_get_strategy
        self.logger.success("patched get_strategy")

    async def patch_get_assignment(self):
        async def new_get_assignment(device_id: int) -> Optional[str]:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                data = await self._get_account_info(origin)
                if not data:
                    return None
                # TODO: extract to method to share with get_account
                auth = SettingsPogoauth(username=data["username"], password=None, level=data["level"],
                                        instance_id=self._db_wrapper.get_instance_id(), device_id=device_id, login_type="ptc",
                                        last_softban_action=None, last_softban_action_location=None)
                return auth

        self._account_handler.get_assignment = new_get_assignment
        self.logger.success("patched get_assignment")


    def __init__(self, subapp_to_register_to: web.Application, mad_parts: Dict):
        super().__init__(subapp_to_register_to, mad_parts)

        self._rootdir = os.path.dirname(os.path.abspath(__file__))
        self._mad = self._mad_parts
        self.logger = self._mad['logger']
        self.mm = self._mad['mapping_manager']
        self._account_handler = self.mm._MappingManager__account_handler
        self._assignment_lock = self._account_handler._assignment_lock
        self._db_wrapper = self._account_handler._db_wrapper
        self.mitm_mapper = self._mad['mitm_mapper']
        self.strategy_factory = self._mad['ws_server']._WebsocketServer__strategy_factory

        statusname = self._mad["args"].status_name
        self.logger.info("Got statusname: {}", statusname)
        if os.path.isfile(self._rootdir + "/plugin-" + statusname + ".ini"):
            self._pluginconfig.read(self._rootdir + "/plugin-" + statusname + ".ini")
            self.logger.info("loading instance-specific config for {}", statusname)
        else:
            self._pluginconfig.read(self._rootdir + "/plugin.ini")
            self.logger.info("loading standard plugin.ini")

        self._versionconfig.read(self._rootdir + "/version.mpl")
        self.author = self._versionconfig.get("plugin", "author", fallback="unknown")
        self.url = self._versionconfig.get("plugin", "url", fallback="https://www.maddev.eu")
        self.description = self._versionconfig.get("plugin", "description", fallback="unknown")
        self.version = self._versionconfig.get("plugin", "version", fallback="unknown")
        self.pluginname = self._versionconfig.get("plugin", "pluginname", fallback="https://www.maddev.eu")
        self.staticpath = self._rootdir + "/static/"
        self.templatepath = self._rootdir + "/template/"

        # plugin specific
        self.server_host = self._pluginconfig.get(statusname, "server_host", fallback="127.0.0.1")
        self.server_port = self._pluginconfig.getint(statusname, "server_port", fallback=9008)
        self.region = self._pluginconfig.get(statusname, "region", fallback="")
        global_auth_username = self._pluginconfig.get("plugin", "auth_username", fallback=None)
        global_auth_password = self._pluginconfig.get("plugin", "auth_password", fallback=None)
        self.auth_username = self._pluginconfig.get(statusname, "auth_username", fallback=global_auth_username)
        self.auth_password = self._pluginconfig.get(statusname, "auth_password", fallback=global_auth_password)

        self.__worker_strategy = dict()
        self.__worker_encounter_check_interval_sec = self._pluginconfig.getint(statusname, "encounter_check_interval", fallback=30 * 60)
        self.__encounter_limit = self._pluginconfig.getint(statusname, "encounter_limit", fallback=5000)
        self.__excluded_workers = ['ing21x64', 'ing20x64', 'ing18x64', 'ing16x64']

        if self.auth_username and self.auth_password:
            auth = aiohttp.BasicAuth(self.auth_username, self.auth_password)
        else:
            auth = None
        self.session = aiohttp.ClientSession(auth=auth)

        # linking pages
        self._hotlink = [
            ("accountServerConnector Manual", "accountserver_manual", "accountServerConnector Manual"),
        ]

        if self._pluginconfig.getboolean("plugin", "active", fallback=False):
            register_custom_plugin_endpoints(self._plugin_subapp)

            for name, link, description in self._hotlink:
                self._mad_parts['madmin'].add_plugin_hotlink(name, link.replace("/", ""),
                                                             self.pluginname, self.description, self.author, self.url,
                                                             description, self.version)

    async def _perform_operation(self):
        if not self._pluginconfig.getboolean("plugin", "active", fallback=False):
            return False
        await self.patch_set_level()
        await self.patch_get_account()
        await self.patch_notify_logout()
        await self.patch_mark_burnt()
        await self.patch_is_burnt()
        await self.patch_get_assigned_username()
        await self.patch_set_last_softban_action()
        await self.patch_get_assignment()

        loop = asyncio.get_running_loop()
        self.__worker_encounter_limit_check = loop.create_task(self._check_encounters())

        return True

    async def _check_encounters(self):
        while True:
            async with self._db_wrapper as session, session:
                counters = await self._count_by_worker(session)
                for worker, count in counters.items():
                    if count < self.__encounter_limit:
                        continue
                    if worker in self.__worker_strategy:
                        if not worker in self.__excluded_workers:
                            self.logger.warning(f"Switching worker {worker} as #encounters have reached {count} (> {self.__encounter_limit})")
                            try:
                                await self.__worker_strategy[worker]._switch_user('limit')
                            except Exception:
                                self.logger.opt(exception=True).error("Exception while switching user")
                        else:
                            self.logger.info(f"Worker {worker} is excluded from encounter_limit based account switching")
                    else:
                        self.logger.warning(f"Unable to switch user on worker {worker} as strategy instance is missing")

            await asyncio.sleep(self.__worker_encounter_check_interval_sec)

    async def _count_by_worker(self, session: AsyncSession, worker: str=None) -> Any:
        logger.debug("Getting # encounters")
        worker_count: Dict[str, int] = {}
        try:
            stmt = select(
                TrsStatsDetectWildMonRaw.worker,
                func.count("*")) \
                .select_from(TrsStatsDetectWildMonRaw) \
                .group_by(TrsStatsDetectWildMonRaw.worker)
            result = await session.execute(stmt)
            for db_worker, count in result.all():
                if worker and worker != db_worker:
                    continue
                worker_count[db_worker] = count
        except Exception:
            self.logger.opt(exception=True).error(f"Exception while getting number of encounters for {worker}")
        if worker and not worker in worker_count:
            worker_count[worker] = 0
        return worker_count[worker] if worker else worker_count

    @staticmethod
    async def _delete_worker_stats(session: AsyncSession, worker: str) -> None:
        try:
            logger.info(f"Deleting worker stats for {worker}")
            stmt = delete(TrsStatsDetectWildMonRaw) \
                .where(TrsStatsDetectWildMonRaw.worker == worker)
            await session.execute(stmt)
            logger.info("Deleted worker stats for " + worker)
            await session.commit()
        except Exception:
            logger.opt(exception=True).error("Exception while deleting worker stats")

    async def _available_accounts(self, origin):
        params = {'device': origin, 'leveling': 1 if await self.mm.routemanager_of_origin_is_levelmode(origin) else 0, 'region': self.region}
        r, content = await self.__get(f"/get/availability", params)
        return content['available'] if r and r.status == 200 else 0

    async def _request_account(self, origin: str, level_mode: bool, reason=None):
        self.logger.info(f"Try to get account for {origin}")
        params = {'region': self.region, 'leveling': 1 if level_mode else 0}
        if reason:
            params['reason'] = reason
        r, content = await self.__get(f"/get/{origin}", params)
        return content if r and r.status == 200 else None

    async def _get_account_info(self, origin: str) -> Optional[str]:
        self.logger.info(f"Try to get account_info of {origin}")
        r, content = await self.__get(f"/get/{origin}/info")
        return content if r and r.status == 200 else None

    async def _track_level(self, origin: str, level: int) -> bool:
        self.logger.debug(f"Setting level {level} for origin {origin}")
        r, _ = await self.__post(f"/set/{origin}/level/{level}")
        return r and r.ok

    async def _logout(self, origin: str, encounters: Optional[int]) -> bool:
        data = {}
        if encounters:
            data['encounters'] = encounters
        self.logger.info(f"Logging out account for origin {origin} with data {str(data)}")
        r, _ = await self.__post(f"/set/{origin}/logout", data)
        return r and r.ok

    async def _burn_account(self, origin: str, reason: str = None, encounters: int = None):
        data = {}
        if reason:
            data['reason'] = reason
        if encounters:
            data['encounters'] = encounters
        self.logger.info(f"Burning account of origin {origin} with data {str(data)}")
        r, _ = await self.__post(f"/set/{origin}/burned", data)
        return r and r.ok

    async def __get(self, endpoint: str, params: Any = None):
        url = f"http://{self.server_host}:{self.server_port}{endpoint}"
        try:
            async with self.session.get(url, params=params) as r:
                content = await r.content.read()
                content = content.decode()
                if r.ok:
                    if content:
                        content = json.loads(content)["data"]
                    self.logger.debug(f"Request ok, response: {content}")
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                return r, content
        except Exception as e:
            self.logger.exception(f"Exception trying to run request at {url}: {e}")
            return None, None

    async def __post(self, endpoint: str, data: Any = None):
        url = f"http://{self.server_host}:{self.server_port}{endpoint}"
        try:
            async with self.session.post(url, json=data) as r:
                content = await r.content.read()
                content = content.decode()
                if r.ok:
                    try:
                        if r.status != 204 and content:
                            content = json.loads(content)["data"]
                    except Exception:
                        pass
                    self.logger.debug(f"Request ok, response: {content}")
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                return r, content
        except Exception as e:
            self.logger.exception(f"Exception trying to run request at {url}: {e}")
            return None, None
