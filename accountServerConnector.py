import asyncio
import datetime
import json
import os
import time
from typing import (Any, Dict, Optional)

import aiohttp
import mapadroid.plugins.pluginBase
from aiocache import cached
from aiohttp import web
from loguru import logger
from mapadroid.account_handler.AbstractAccountHandler import (
    AccountPurpose, BurnType)
from mapadroid.db.helper.SettingsDeviceHelper import SettingsDeviceHelper
from mapadroid.db.model import SettingsDevice, SettingsPogoauth
from mapadroid.db.model import TrsStatsDetectWildMonRaw
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper
from mapadroid.utils.collections import Location
from mapadroid.utils.madGlobals import InternalStopWorkerException
from plugins.accountServerConnector.endpoints import register_custom_plugin_endpoints
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select


# TODO: track purpose through job as it may change e.g. quest -> iv
class accountServerConnector(mapadroid.plugins.pluginBase.Plugin):
    """accountServerConnector plugin
    """

    def _file_path(self) -> str:
        return os.path.dirname(os.path.abspath(__file__))

    async def patch_ah_set_level(self):
        async def new_set_level(device_id: int, level: int):
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                logger.info("Setting level of {} to {}", origin, level)
                await self._track_level(device_entry.name, level)

        self._account_handler.set_level = new_set_level
        logger.success("patched set_level")

    async def patch_ah_get_account(self):
        async def new_get_account(device_id: int, purpose: AccountPurpose,
            location_to_scan: Optional[Location],
            including_google: bool = True) -> Optional[SettingsPogoauth]:
            async with self._assignment_lock:
                async with self._db_wrapper as session, session:
                    device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                        session, self._db_wrapper.get_instance_id(), device_id)
                    if not device_entry:
                        logger.warning("Device ID {} not found in device table", device_id)
                        return None
                    origin = device_entry.name
                    account_info = await self._request_account(origin, purpose=purpose, location=location_to_scan, reason='prelogin')
                    if not account_info:
                        return None
                    stat_entries = await self._count_worker_stats(session, origin)
                    if stat_entries > 0:
                        logger.warning(f"Found {stat_entries} worker stat entries ")
                    self._extract_remaining_encounters(origin, account_info)
                    softban_info = self._extract_softban_info(account_info)
                    auth = SettingsPogoauth(username=account_info["username"], password=account_info["password"], level=account_info["level"],
                                            instance_id=self._db_wrapper.get_instance_id(), device_id=device_id, login_type="ptc",
                                            last_softban_action=softban_info[0] if softban_info else None, last_softban_action_location=softban_info[1] if softban_info else None)
                    logger.debug(f"Returning auth {auth}")
                    return auth
            return None

        self._account_handler.get_account = new_get_account
        logger.success("patched get_account")

    async def patch_ah_notify_logout(self):
        async def new_notify_logout(device_id: int) -> None:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name

                encounters = await self._count_by_worker(session, worker=origin)
                level = await self.mitm_mapper.get_level(origin)

                logger.debug("Saving logout of {} with {} encounters and level {}", origin, encounters, level)
                await self._delete_worker_stats(session, origin)
                await self._logout(origin, encounters, level)

        self._account_handler.notify_logout = new_notify_logout
        logger.success("patched notify_logout")

    # only used for MAD triggered BurnTypes
    async def patch_ah_mark_burnt(self):
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
                level = await self.mitm_mapper.get_level(origin)
                await self._burn_account(origin, reason=burn_type.value, encounters=encounters, level=level)
                await self._delete_worker_stats(session, origin)

        self._account_handler.mark_burnt = new_mark_burnt
        logger.success("patched mark_burnt")

    async def patch_ah_get_assigned_username(self):
        async def new_get_assigned_username(device_id: int) -> Optional[str]:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                account_info = await self._get_account_info(origin)
                if not account_info:
                    return None
                self._extract_remaining_encounters(origin, account_info)
                return account_info["username"]

        self._account_handler.get_assigned_username = new_get_assigned_username
        logger.success("patched get_assigned_username")

    async def patch_ah_set_last_softban_action(self):
        async def new_set_last_softban_action(device_id: int, time_of_action: datetime.datetime,
            location_of_action: Location) -> None:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                try:
                    await self._set_last_softban_action(origin, time_of_action, location_of_action)
                except Exception as ex:
                    logger.warning("Unable to set softban info: {}", ex)
                    pass

        self._account_handler.set_last_softban_action = new_set_last_softban_action
        logger.success("patched set_last_softban_action")

    async def patch_ah_is_burnt(self):
        async def new_is_burnt(device_id: int) -> bool:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return False
                origin = device_entry.name
                logger.debug("Checking whether account is burnt")
                account_info = await self._get_account_info(origin)
                if not account_info:
                    logger.warning(f"Unable to check if account is_burnt as there is currently no assignment")
                    return False
                return True if int(account_info["is_burnt"]) else False
            return False

        self._account_handler.is_burnt = new_is_burnt
        logger.success("patched is_burnt")

    async def patch_mm_handle_login_request(self):
        old_ip_handle_login_request = self.mm.ip_handle_login_request

        async def new_ip_handle_login_request(ip, origin, limit_seconds=None, limit_count=None):
            account_info = await self._get_account_info(origin)
            if not account_info:
                logger.info(f"ip_handle_login_request -> Skip IP tracking due to missing (known) assignment")
                # no assignment, assume pogo app has no cached login so no need to track login right now
                received_slot = True
            else:
                logger.debug(f"ip_handle_login_request -> Regular login tracking")
                self._extract_remaining_encounters(origin, account_info)
                received_slot = await old_ip_handle_login_request(ip, origin, limit_seconds, limit_count)
                if received_slot:
                    await self._track_login(origin)

            return received_slot

        self.mm.ip_handle_login_request = new_ip_handle_login_request
        logger.success("patched ip_handle_login_request")

    async def patch_sf_get_strategy(self):
        old_get_strategy = self.strategy_factory.get_strategy

        async def new_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state):
            strategy = await old_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state)

            # intercept switch_user for switch reason
            async def new_switch_user(reason=None):
                # reason oneof ['maintenance', 'limit', 'level', 'teleport']
                origin = worker_state.origin
                logger.info(f"custom _switch_user(origin={origin}, reason={reason})")
                async with self._db_wrapper as session, session:
                    encounters = await self._count_by_worker(session, worker=origin)
                    ## mostly copied from original "_switch_user"
                    # stop and wait for remaining data to be processed
                    await strategy.stop_pogo()
                    await asyncio.sleep(5)
                    # maintenance is handled already through "mark_burnt"
                    if reason != BurnType.MAINTENANCE.value or encounters > 0:
                        # set_level may not have been called before, forcing update
                        level = await self.mitm_mapper.get_level(origin)
                        # must burn account before calling clear_game_data, which triggers logout
                        await self._burn_account(origin, reason=reason, encounters=encounters, level=level)
                        await self._delete_worker_stats(session, origin)
                    await strategy._clear_game_data()
                    await asyncio.sleep(5)
                    await strategy.turn_screen_on_and_start_pogo()
                    if not await strategy._ensure_pogo_topmost():
                        logger.error('Kill Worker...')
                        raise InternalStopWorkerException("Pogo not topmost app during switching of users")
                    logger.info('Switching finished ...')
                    return True

            if strategy._switch_user and strategy._switch_user != new_switch_user:
                logger.debug("patch _switch_user for " + strategy.__class__.__name__)
                strategy._switch_user = new_switch_user
                self.__worker_strategy[worker_state.origin] = strategy
            elif strategy._switch_user == new_switch_user:
                logger.warning("already patched switch_user")
            else:
                logger.warning("unable to patch switch_user")

            old_check_ptc_login_ban = strategy._word_to_screen_matching.check_ptc_login_ban

            async def new_check_ptc_login_ban():
                purpose = await self.mm.routemanager_get_purpose_of_device(worker_state.area_id)
                # Suppress login attempt when no account is available
                count = await self._available_accounts(worker_state.origin, purpose)
                allow_login = count > 0 and await old_check_ptc_login_ban()
                if not allow_login:
                    logger.warning(f"Accounts available for {purpose}: {count > 0}. {'Allowing' if allow_login else 'Preventing'} PTC login.")
                return allow_login

            if strategy._word_to_screen_matching.check_ptc_login_ban != new_check_ptc_login_ban:
                logger.debug("patch check_ptc_login_ban")
                strategy._word_to_screen_matching.check_ptc_login_ban = new_check_ptc_login_ban
            return strategy

        self.strategy_factory.get_strategy = new_get_strategy
        logger.success("patched get_strategy")

    async def patch_ah_get_assignment(self):
        async def new_get_assignment(device_id: int) -> Optional[SettingsPogoauth]:
            async with self._db_wrapper as session, session:
                device_entry: Optional[SettingsDevice] = await SettingsDeviceHelper.get(
                    session, self._db_wrapper.get_instance_id(), device_id)
                if not device_entry:
                    logger.warning("Device ID {} not found in device table", device_id)
                    return None
                origin = device_entry.name
                account_info = await self._get_account_info(origin)
                if not account_info:
                    return None
                self._extract_remaining_encounters(origin, account_info)
                softban_info = self._extract_softban_info(account_info)
                # TODO: extract to method to share with get_account
                auth = SettingsPogoauth(username=account_info["username"], password=None, level=account_info["level"],
                                        instance_id=self._db_wrapper.get_instance_id(), device_id=device_id, login_type="ptc",
                                        last_softban_action=softban_info[0] if softban_info else None, last_softban_action_location=softban_info[1] if softban_info else None)
                return auth

        self._account_handler.get_assignment = new_get_assignment
        logger.success("patched get_assignment")

    def __init__(self, subapp_to_register_to: web.Application, mad_parts: Dict):
        super().__init__(subapp_to_register_to, mad_parts)

        self._rootdir = os.path.dirname(os.path.abspath(__file__))
        self._mad = self._mad_parts
        self.mm = self._mad['mapping_manager']
        self._account_handler = self.mm._MappingManager__account_handler
        self._assignment_lock = self._account_handler._assignment_lock
        self._db_wrapper = self._account_handler._db_wrapper
        self.mitm_mapper = self._mad['mitm_mapper']
        self.strategy_factory = self._mad['ws_server']._WebsocketServer__strategy_factory

        statusname = self._mad["args"].status_name
        logger.info("Got statusname: {}", statusname)
        if os.path.isfile(self._rootdir + "/plugin-" + statusname + ".ini"):
            self._pluginconfig.read(self._rootdir + "/plugin-" + statusname + ".ini")
            logger.info("loading instance-specific config for {}", statusname)
        else:
            self._pluginconfig.read(self._rootdir + "/plugin.ini")
            logger.info("loading standard plugin.ini")

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
        self.__switching_workers: list[str] = list()
        self.__remaining_encounters: dict[str, int] = dict()
        self.__worker_encounter_check_interval_sec = self._pluginconfig.getint(statusname, "encounter_check_interval", fallback=30 * 60)
        self.__excluded_workers = [x.strip(' ') for x in self._pluginconfig.get(statusname, "excluded_workers", fallback='').split(",")]

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
        await self.patch_sf_get_strategy()
        await self.patch_mm_handle_login_request()
        await self.patch_ah_set_level()
        await self.patch_ah_get_account()
        await self.patch_ah_notify_logout()
        await self.patch_ah_mark_burnt()
        await self.patch_ah_is_burnt()
        await self.patch_ah_get_assigned_username()
        await self.patch_ah_set_last_softban_action()
        await self.patch_ah_get_assignment()

        loop = asyncio.get_running_loop()
        self.__worker_encounter_limit_check = loop.create_task(self._check_encounters())

        return True

    async def _check_encounters(self):
        async with self._db_wrapper as session, session:
            encounters = await self._count_by_worker(session)
            for worker, _ in encounters.items():
                account_info = await self._get_account_info(worker)
                if not account_info:
                    continue
                self._extract_remaining_encounters(worker, account_info)

        def switch_done_callback(worker, task_start):
            with logger.contextualize(identifier=worker, name="worker"):
                duration = time.time() - task_start
                if duration > 30:
                    logger.info(f"Switching of {worker} finished after {int(duration)}s")
                if worker in self.__switching_workers:
                    self.__switching_workers.remove(worker)

        while True:
            try:
                async with self._db_wrapper as session, session:
                    encounters = await self._count_by_worker(session)
                    logger.info(f"Running encounter limit job: {encounters}")
                    loop = asyncio.get_running_loop()

                    for worker, count in encounters.items():
                        with logger.contextualize(identifier=worker, name="worker"):
                            if not worker in self.__remaining_encounters or count < self.__remaining_encounters[worker]:
                                continue
                            if worker in self.__switching_workers:
                                logger.info(f"Worker {worker} is just switching. Ignoring")
                                continue
                            if worker in self.__worker_strategy:
                                if not worker in self.__excluded_workers:
                                    logger.warning(f"Switching worker {worker} as #encounters have reached {count} (> {self.__remaining_encounters[worker]})")
                                    task = loop.create_task(self._switch_user_due_to_limit(worker))
                                    self.__switching_workers.append(worker)
                                    task_start = time.time()
                                    task.add_done_callback(lambda t: switch_done_callback(worker, task_start))
                                else:
                                    logger.info(f"Worker {worker} is excluded from encounter_limit based account switching")
                            else:
                                logger.warning(f"Unable to switch user on worker {worker} due to encounter_limit as strategy instance is missing")

            except Exception as ex:
                logger.exception(ex)
            logger.info(f"Sleeping for {self.__worker_encounter_check_interval_sec} before next encounter limit check")
            await asyncio.sleep(self.__worker_encounter_check_interval_sec)

    async def _switch_user_due_to_limit(self, worker):
        try:
            async with asyncio.timeout(30 * 60):
                await self.__worker_strategy[worker]._switch_user('limit')
        except Exception as ex:
            logger.exception(ex)
            logger.opt(exception=True).error(f"Exception while switching user of {worker}")

    async def _count_by_worker(self, session: AsyncSession, worker: str = None) -> Any:
        logger.debug("Getting # encounters")
        worker_count: Dict[str, int] = {}
        try:
            stmt = select(
                TrsStatsDetectWildMonRaw.worker,
                func.count("*")) \
                .select_from(TrsStatsDetectWildMonRaw)
            if worker:
                stmt = stmt.where(TrsStatsDetectWildMonRaw.worker == worker)
            stmt = stmt \
                .where(TrsStatsDetectWildMonRaw.last_scanned > (DatetimeWrapper.now() - datetime.timedelta(days=1))) \
                .group_by(TrsStatsDetectWildMonRaw.worker)
            result = await session.execute(stmt)
            for db_worker, count in result.all():
                worker_count[db_worker] = count
        except Exception:
            logger.opt(exception=True).error(f"Exception while getting number of encounters for {worker}")
        if worker and not worker in worker_count:
            worker_count[worker] = 0
        return worker_count[worker] if worker else worker_count

    def _extract_softban_info(self, account_info: dict[str, any]) -> tuple[datetime, Location]:
        softban_info = None
        try:
            if 'softban_info' in account_info:
                # logger.info(f"Softban info: {account_info['softban_info']}")
                time = datetime.datetime.fromisoformat(account_info["softban_info"]["time"])
                location = Location.from_json(account_info["softban_info"]["location"])
                softban_info = (time, location)
                logger.debug(f"softban_info time from server {time} for {account_info['username']}")
        except Exception as ex:
            logger.warning("Unable to extract softban_info: {}", ex)
        return softban_info

    def _extract_remaining_encounters(self, origin, account_info) -> int:
        if not account_info:
            return 9999999
        if "remaining_encounters" in account_info:
            old = self.__remaining_encounters[origin] if origin in self.__remaining_encounters else None
            new = int(account_info["remaining_encounters"])
            self.__remaining_encounters[origin] = new
            username = account_info["username"]
            if not old or old != new:
                logger.info(f"Updated remaining encounters for {origin}@{username} from {old if old else '-'} to {new}")
            return self.__remaining_encounters[origin]
        return 99999999

    async def _count_worker_stats(self, session: AsyncSession, worker: str) -> int:
        try:
            count = select(func.count('*')) \
                .select_from(TrsStatsDetectWildMonRaw) \
                .where(TrsStatsDetectWildMonRaw.worker == worker)
            result = await session.execute(count)
            count = result.scalar_one()
            if count:
                return int(count)
        except Exception as ex:
            logger.opt(exception=True).error(f"Exception while deleting worker stats: {ex}")
        return 0

    async def _delete_worker_stats(self, session: AsyncSession, worker: str) -> None:
        try:
            count = await self._count_worker_stats(session, worker)
            if count > 0:
                delete_stmt = delete(TrsStatsDetectWildMonRaw) \
                    .where(TrsStatsDetectWildMonRaw.worker == worker)
                await session.execute(delete_stmt)
                await session.commit()
                logger.info(f"Deleted {count} worker stats")
            else:
                logger.debug("No worker stats to delete")
            if worker in self.__remaining_encounters:
                del self.__remaining_encounters[worker]
        except Exception as ex:
            logger.opt(exception=True).error(f"Exception while deleting worker stats: {ex}")

    async def _available_accounts(self, origin: str, purpose: AccountPurpose = None):
        params = {'device': origin, 'region': self.region, 'purpose': purpose.value}
        r, content = await self.__get(f"/get/availability", params)
        return content['available'] if r and r.status == 200 else 0

    async def _request_account(self, origin: str, purpose: AccountPurpose, reason=None, location: Optional[Location] = None):
        logger.debug(f"Try to get account for {origin}")
        data = {'region': self.region}
        if reason:
            data['reason'] = reason
        if location:
            data['location'] = [location.lat, location.lng]
        if location:
            data['purpose'] = purpose.value
        logger.debug(f"_request_account: {data}")
        r, content = await self.__post(f"/get/{origin}", data)
        return content if r and r.status == 200 else None

    @cached(ttl=5)
    async def _get_account_info(self, origin: str) -> Optional[dict[str, Any]]:
        logger.debug(f"Try to get account_info of {origin}")
        r, content = await self.__get(f"/get/{origin}/info")
        return content if r and r.status == 200 else None

    async def _track_level(self, origin: str, level: int) -> bool:
        logger.debug(f"Setting level {level} for origin {origin}")
        r, _ = await self.__post(f"/set/{origin}/level/{level}")
        return r and r.ok

    async def _track_login(self, origin: str) -> bool:
        logger.debug(f"Tracking login of {origin}")
        r, _ = await self.__post(f"/set/{origin}/login")
        return r and r.ok

    async def _logout(self, origin: str, encounters: Optional[int], level: Optional[int]) -> bool:
        data = {}
        if encounters:
            data['encounters'] = encounters
        if level:
            data['level'] = level
        if len(data) > 0:
            logger.debug(f"Logging out. Data: {str(data)}")
        r, _ = await self.__post(f"/set/{origin}/logout", data)
        return r and r.ok

    async def _burn_account(self, origin: str, reason: str = None, encounters: int = None, level: int = None):
        data = {}
        if reason:
            data['reason'] = reason
        if encounters:
            data['encounters'] = encounters
        if level:
            data['level'] = level
        logger.info(f"Burning account of origin {origin} with data {str(data)}")
        r, _ = await self.__post(f"/set/{origin}/burned", data)
        return r and r.ok

    async def _set_last_softban_action(self, origin: str, time_of_action: datetime, location: Location):
        data = {"time": time_of_action.isoformat(), "location": [location.lat, location.lng]}
        logger.debug(f"Setting softban of origin {origin} with data {str(data)}")
        r, _ = await self.__post(f"/set/{origin}/softban", data)
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
                    logger.debug(f"Request ok, response: {content}")
                else:
                    logger.warning(f"Request NOT ok, response: {content}")
                return r, content
        except Exception as e:
            logger.exception(f"Exception trying to run request at {url}: {e}")
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
                    logger.debug(f"Request ok, response: {content}")
                else:
                    logger.warning(f"Request NOT ok, response: {content}")
                return r, content
        except Exception as e:
            logger.exception(f"Exception trying to run request at {url}: {e}")
            return None, None
