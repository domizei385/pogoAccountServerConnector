from mapadroid.plugins.endpoints.AbstractPluginEndpoint import AbstractPluginEndpoint
import aiohttp_jinja2


class accountServerManualEndpoint(AbstractPluginEndpoint):
    """
    "/accountserver_manual"
    """

    # TODO: Auth
    @aiohttp_jinja2.template('accountserver_manual.html')
    async def get(self):
        return {"header": "accountServerConnector Manual",
                "title": "accountServerConnector Manual"}
