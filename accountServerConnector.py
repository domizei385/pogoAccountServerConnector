import os
import json
from enum import Enum
from typing import Dict
from aiohttp import web
import aiohttp

import mapadroid.plugins.pluginBase
from plugins.accountServerConnector.endpoints import register_custom_plugin_endpoints
from mapadroid.utils.collections import Login_PTC
from mapadroid.mapping_manager.MappingManagerDevicemappingKey import \
    MappingManagerDevicemappingKey


class accountServerConnector(mapadroid.plugins.pluginBase.Plugin):
    """accountServerConnector plugin
    """

    def _file_path(self) -> str:
        return os.path.dirname(os.path.abspath(__file__))

    async def patch_get_strategy(self):
        self.logger.info("try to patch get_strategy")
        old_get_strategy = self.strategy_factory.get_strategy
        async def new_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state):
            async def new_get_next_account(origin=worker_state.origin):
                return await self.request_account(origin)
            strategy = await old_get_strategy(worker_type, area_id, communicator, walker_settings, worker_state)
            logintype = await self.mm.get_devicesetting_value_of_device(worker_state.origin,
                                                                        MappingManagerDevicemappingKey.LOGINTYPE)
            if logintype == "ptc" and strategy._word_to_screen_matching.get_next_account != new_get_next_account:
                self.logger.info(f"patch get_next_account for {worker_state.origin} using PTC accounts")
                strategy._word_to_screen_matching.get_next_account = new_get_next_account
            elif strategy._word_to_screen_matching.get_next_account == new_get_next_account:
                self.logger.warning(f"already patched for {worker_state.origin}")
            else:
                self.logger.info(f"not patching for {worker_state.origin} - logintype is {logintype}")
            return strategy
        self.strategy_factory.get_strategy = new_get_strategy
        self.logger.success("patched get_strategy / get_next_account!")

    def __init__(self, subapp_to_register_to: web.Application, mad_parts: Dict):
        super().__init__(subapp_to_register_to, mad_parts)

        self._rootdir = os.path.dirname(os.path.abspath(__file__))
        self._mad = self._mad_parts
        self.logger = self._mad['logger']
        self.mm = self._mad['mapping_manager']
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
        global_auth_username = self._pluginconfig.get("plugin", "auth_username", fallback=None)
        global_auth_password = self._pluginconfig.get("plugin", "auth_password", fallback=None)
        self.auth_username = self._pluginconfig.get(statusname, "auth_username", fallback=global_auth_username)
        self.auth_password = self._pluginconfig.get(statusname, "auth_password", fallback=global_auth_password)

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
        return True

    async def request_account(self, origin):
        url = f"http://{self.server_host}:{self.server_port}/get/{origin}"
        self.logger.info(f"Try to get account from: {url}")
        try:
            async with self.session.get(url) as r:
                content = await r.content.read()
                content = content.decode()
                if r.ok:
                    self.logger.info(f"Request ok, response: {content}")
                    j = json.loads(content)
                    username = j["data"]["username"]
                    password = j["data"]["password"]
                    return Login_PTC(username, password)
                else:
                    self.logger.warning(f"Request NOT ok, response: {content}")
                    return False
        except Exception as e:
            self.logger.exception(f"Exception trying to request account from account server: {e}")
            return False
