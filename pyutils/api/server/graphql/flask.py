import importlib.util
from json import JSONEncoder as _JSONEncoder
from pathlib import Path
from typing import Callable

from ariadne.explorer import ExplorerPlayground
from flask import Flask, Response, jsonify, request
from flask_limiter import Limiter

from pyutils.api.server.graphql.general import generate_response, validate_request
from pyutils.config.providers import YAMLConfigProvider


def update_headers(resp: Response, headers: dict):
    resp_header = resp.headers
    for header, value in headers.items():
        # Override header value
        resp_header[header] = value
    return resp


def add_headers(resp: Response, api_version: str) -> Response:
    """
    Add response headers.
    """
    no_cache_headers = {
        "Cache-Control": "max-age=0, must-revalidate, no-cache, no-store, public",
        "Pragma": "no-cache",
        "Expires": "0",  # [mozilla]/en-US/docs/Web/HTTP/Headers/Expires
    }
    update_headers(resp, no_cache_headers)

    # Update response header with api version
    version_header = {"X-Api-Version": api_version}
    update_headers(resp, version_header)

    return resp


def attach_graphql_playground_route(app: Flask, environment: str, api_version: str):
    """
    Given a Flask app, attaches a route that serves a GraphQL Playground on GET.
    """

    @app.route("/graphql", methods=["GET"])
    def graphql_playground():
        # Use Class based GraphQL IDE Generator
        ide_title = f"[{environment}] GraphQL App: {api_version}"
        ide_settings = {
            "editor_reuse_headers": True,
            "general_beta_updates": False,
            "prettier_tab_width": 2,
            "prettier_use_tabs": False,
            "schema_polling_enable": False,
        }
        ide_obj = ExplorerPlayground(title=ide_title, **ide_settings)
        ide_html = ide_obj.html(None)
        return ide_html, 200

    return graphql_playground


def attach_graphql_server_route(
    app, graphql_schema, auth_decorator=None,
    limiter: Limiter = None,
    limiter_func: Callable = None,
    **kwargs
):
    """
    Given a Flask app and an executable Ariadne GraphQL schema, attaches a route that
    does the following upon a POST request:
        1. Authenticates requests
        2. Dispatches requests to GraphQL resolvers and mutations
        3. Returns a JSON response
    """

    def graphql_server():
        """Serve GraphQL queries"""
        response = {}

        is_bad_request = validate_request(request)
        if is_bad_request:
            message = (
                jsonify(is_bad_request)
                if not isinstance(is_bad_request, str)
                else is_bad_request
            )
            return message, 400

        response = generate_response(graphql_schema, request, **kwargs)
        return jsonify(response)

    # Apply authentication decorator if provided
    if auth_decorator:
        graphql_server = auth_decorator(graphql_server)
    else:
        graphql_server = graphql_server

    # Apply rate limiter if provided
    if limiter and limiter_func:
        graphql_server = limiter.limit(limiter_func)(graphql_server)
    else:
        graphql_server = graphql_server

    # Apply flask route decorator
    app.route("/graphql", methods=["POST"])(graphql_server)

    return graphql_server


########################################################################################
# Functions to dynamically load Python modules in the resolver and mutation directories,
# allowing. The reason why this is done is because the decorators supplied on the
# resolver and mutation functions need to be evaluated before the executable schema for
# Ariadne is defined.
#
# Leveraging this dynamic import model makes things easier for our users because they
# can just define directories where they keep their resolvers and mutations and we can
# automatically load the modules present in those directories.
########################################################################################
def load_module_from_filename(filename: str):
    """
    Attempts to load the Python module defined by the specified filename
    """
    # "/directory/module.py" -> "module"
    module_name = str(filename).split("/")[-1].rstrip(".py")
    spec = importlib.util.spec_from_file_location(module_name, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def load_modules_in_directory(directory):
    """
    Given a directory, will load all of the Python files present in that directory.
    """
    python_files = Path(directory).glob("*.py")
    for filename in python_files:
        load_module_from_filename(filename)


def load_resolvers_and_mutations(graphql_config_location: str):
    """
    Retrieves the configured resolver and mutation locations and loads the modules in
    those directories so they can be correctly initialized in Ariadne.
    """
    provider = YAMLConfigProvider(graphql_config_location)
    resolver_locations = provider.provide(["resolver_locations"], secret=False)
    mutation_locations = provider.provide(["mutation_locations"], secret=False)
    for directory in resolver_locations or []:
        load_modules_in_directory(directory)
    for directory in mutation_locations or []:
        load_modules_in_directory(directory)


class JSONEncoder(_JSONEncoder):
    """Custom JSONEncoder for jsonify"""

    def __init__(self, *args, **kwargs):
        # Set default to str for unsupported types
        kwargs["default"] = str
        super().__init__(*args, **kwargs)
