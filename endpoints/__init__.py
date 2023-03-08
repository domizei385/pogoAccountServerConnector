from aiohttp import web

from plugins.accountServerConnector.endpoints.accountServerManualEndpoint import accountServerManualEndpoint


def register_custom_plugin_endpoints(app: web.Application):
    # Simply register any endpoints here. If you do not intend to add any views (which is discouraged) simply "pass"
    app.router.add_view('/accountserver_manual', accountServerManualEndpoint, name='accountserver_manual')
