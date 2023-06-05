import asyncio
import json
import os
from typing import Dict

import aiohttp
import mapadroid.plugins.pluginBase
from aiohttp import web
from mapadroid.db.model import TrsStatsDetectWildMonRaw
from mapadroid.mapping_manager.MappingManagerDevicemappingKey import \
    MappingManagerDevicemappingKey
from mapadroid.utils.collections import Login_PTC
from plugins.accountServerConnector.endpoints import register_custom_plugin_endpoints
from sqlalchemy import and_, delete
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select


class accountServerConnector(mapadroid.plugins.pluginBase.Plugin):
    """accountServerConnector plugin
    """

    def _file_path(self) -> str:
        return os.path.dirname(os.path.abspath(__file__))

    async def patch_get_strategy(self):
        self.logger.info("try to patch get_strategy")
        old_get_strategy = self.strategy_factory.get_strategy

        async def new_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state):
            reasons = {}

            async def new_get_next_account(origin=worker_state.origin):
                reason = None
                if origin in reasons:
                    reason = reasons.pop(origin)
                return await self.request_account(origin, reason)

            strategy = await old_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state)

            logintype = await self.mm.get_devicesetting_value_of_device(worker_state.origin,
                                                                        MappingManagerDevicemappingKey.LOGINTYPE)

            # intercept switch_user for switch reason
            old_switch_user = strategy._switch_user

            async def new_switch_user(reason=None):
                origin = worker_state.origin
                if reason:
                    reasons[origin] = reason
                # TODO: log user out in backend for better timeout handling
                self.logger.info(f"_switch_user(origin={origin}, reason={reason})")
                encounters = 0
                async with self.__db_wrapper as session, session:
                    try:
                        counters = await self.count_by_worker(session, worker=origin)
                        if origin in counters:
                            encounters = counters[origin]
                    except Exception:
                        self.logger.opt(exception=True).error(f"Exception while getting number of encounters for {origin}")
                if reason == 'maintenance' or reason == 'limit':
                    await self.burn_account(origin, reason=reason, encounters=encounters)
                await old_switch_user(reason)

            if logintype == "ptc" and strategy._switch_user != new_switch_user:
                self.logger.debug("patch _switch_user")
                strategy._switch_user = new_switch_user
                self.__worker_strategy[worker_state.origin] = strategy
            elif strategy._switch_user == new_switch_user:
                self.logger.warning("already patched switch_user")

            if logintype == "ptc":
                if strategy._word_to_screen_matching.get_next_account != new_get_next_account:
                    self.logger.debug(f"patch get_next_account for {worker_state.origin} using PTC accounts")
                    strategy._word_to_screen_matching.get_next_account = new_get_next_account
                else:
                    self.logger.warning(f"already patched for {worker_state.origin}")

                old_track_ptc_login = strategy._word_to_screen_matching.track_ptc_login

                async def new_track_ptc_login():
                    # Suppress login attempt when no account is available
                    count = await self.available_accounts(worker_state.origin)
                    self.logger.info(f"Accounts available for {worker_state.origin}: {str(count)}. Allowing PTC login")
                    return count > 0 and await old_track_ptc_login()
                if strategy._word_to_screen_matching.track_ptc_login != new_track_ptc_login:
                    self.logger.debug("patch track_ptc_login")
                    strategy._word_to_screen_matching.track_ptc_login = new_track_ptc_login
            else:
                self.logger.info(f"not patching for {worker_state.origin} - logintype is {logintype}")
            return strategy

        self.strategy_factory.get_strategy = new_get_strategy
        self.logger.success("patched get_strategy / get_next_account!")

    async def patch_set_level(self):
        self.logger.info("try to patch set_level")
        old_set_level = self.mitm_mapper.set_level

        async def new_set_level(worker: str, level: int) -> None:
            set_level = await old_set_level(worker, level)
            await self.track_level(worker, level)
            return set_level

        self.mitm_mapper.set_level = new_set_level
        self.logger.success("patched set_level!")

    def __init__(self, subapp_to_register_to: web.Application, mad_parts: Dict):
        super().__init__(subapp_to_register_to, mad_parts)

        self._rootdir = os.path.dirname(os.path.abspath(__file__))
        self._mad = self._mad_parts
        self.logger = self._mad['logger']
        self.mm = self._mad['mapping_manager']
        self.mitm_mapper = self._mad['mitm_mapper']
        self.strategy_factory = self._mad['ws_server']._WebsocketServer__strategy_factory
        self.__db_wrapper = self._mad["db_wrapper"]

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
        await self.patch_get_strategy()
        await self.patch_set_level()

        loop = asyncio.get_running_loop()
        self.__worker_encounter_limit_check = loop.create_task(self._check_encounters())

        return True

    async def _check_encounters(self):
        while True:
            self.logger.debug("Getting # encounters")

            async with self.__db_wrapper as session, session:
                try:
                    counters = await self.count_by_worker(session)
                    self.logger.info(str(counters))
                    for worker, count in counters.items():
                        if count > self.__encounter_limit:
                            if worker in self.__worker_strategy:
                                if not worker in self.__excluded_workers:
                                    self.logger.warning(f"Switching worker {worker} as #encounters have reached {count} (> {self.__encounter_limit})")
                                    success = await self.__worker_strategy[worker]._switch_user('limit')
                                    if success:
                                        await self._delete_worker_stats(session, worker)
                                    else:
                                        self.logger.warning(f"Failed to call _switch_user for worker {worker} with strategy {type(self.__worker_strategy[worker]).__name__}")
                                else:
                                    self.logger.info(f"Worker {worker} is excluded from encounter_limit based account switching")
                            else:
                                self.logger.warning(f"Unable to switch user on worker {worker} as strategy instance is missing")
                except Exception:
                    self.logger.opt(exception=True).error("Unhandled exception in pogoAccountServerConnector! Trying to continue... ")

            await asyncio.sleep(self.__worker_encounter_check_interval_sec)

    async def count_by_worker(self, session: AsyncSession, worker: str=None) -> dict[str, int]:
        stmt = select(
            TrsStatsDetectWildMonRaw.worker,
            func.count("*")) \
            .select_from(TrsStatsDetectWildMonRaw) \
            .group_by(TrsStatsDetectWildMonRaw.worker)
        result = await session.execute(stmt)
        worker_count: Dict[str, int] = {}
        for db_worker, count in result.all():
            if worker and worker != db_worker:
                continue
            worker_count[db_worker] = count
        return worker_count

    async def _delete_worker_stats(self, session: AsyncSession, worker: str) -> None:
        self.logger.info(f"Deleting worker stats for {worker}")
        stmt = delete(TrsStatsDetectWildMonRaw) \
            .where(TrsStatsDetectWildMonRaw.worker == worker)
        await session.execute(stmt)

    async def available_accounts(self, origin):
        url = f"http://{self.server_host}:{self.server_port}/stats"
        try:
            async with self.session.get(url) as r:
                content = await r.content.read()
                content = content.decode()
                if r.ok:
                    payload = json.loads(content)
                    self.logger.debug(f"Request ok, response: {payload}")
                    available = payload[self.region]['available']
                    if await self.mm.routemanager_of_origin_is_levelmode(origin):
                        return int(available['unleveled'])
                    else:
                        return int(available['leveled'])
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                    return None
        except Exception as e:
            self.logger.exception(f"Exception trying to request account from account server: {e}")
            return None

    async def request_account(self, origin, reason=None):
        level_mode = await self.mm.routemanager_of_origin_is_levelmode(origin)
        url = f"http://{self.server_host}:{self.server_port}/get/{origin}"
        self.logger.info(f"Try to get account from: {url}")
        try:
            params = {'region': self.region, 'leveling': 1 if level_mode else 0}
            if reason:
                params['reason'] = reason
            async with self.session.get(url, params=params) as r:
                content = await r.content.read()
                content = content.decode()
                if r.status == 200:
                    self.logger.debug(f"Request ok, response: {content}")

                    j = json.loads(content)
                    username = j["data"]["username"]
                    password = j["data"]["password"]
                    return Login_PTC(username, password)
                elif r.status == 204:
                    self.logger.debug("No account available")
                    return None
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                    return None
        except Exception as e:
            self.logger.exception(f"Exception trying to request account from account server: {e}")
            return None

    async def track_level(self, origin: str, level: int):
        url = f"http://{self.server_host}:{self.server_port}/set/{origin}/level/{level}"
        self.logger.debug(f"Setting level {level} for origin {origin}")
        try:
            async with self.session.post(url) as r:
                content = await r.content.read()
                content = content.decode()
                if r.ok:
                    self.logger.debug(f"Request ok, response: {content}")
                    return True
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                    return False
        except Exception as e:
            self.logger.exception(f"Exception trying to set level in account server: {e}")
            return False

    async def burn_account(self, origin: str, reason: str = None, encounters: int = None):
        data = {}
        if reason:
            data['reason'] = reason
        if encounters:
            data['encounters'] = encounters
        url = f"http://{self.server_host}:{self.server_port}/set/{origin}/burned"
        self.logger.info(f"Burning account of origin {origin}")
        try:
            async with self.session.post(url, json=data) as r:
                # TODO: drop stats data for origin, track in history table

                content = await r.content.read()
                content = content.decode()
                if r.ok:
                    self.logger.info(f"Request ok, response: {content}")
                    return True
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                    return False
        except Exception as e:
            self.logger.exception(f"Exception trying to burn account in account server: {e}")
            return False
