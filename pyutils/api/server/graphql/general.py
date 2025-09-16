import inspect
import json
from typing import Any, List, Optional, Union, cast

from ariadne import (
    MutationType,
    QueryType,
    combine_multipart_data,
    format_error,
    graphql_sync,
    load_schema_from_path,
)
from ariadne.types import GraphQLError
from graphql import GraphQLResolveInfo
from graphql.execution import MiddlewareManager
from graphql.language import (
    FieldNode,
    FragmentSpreadNode,
    InlineFragmentNode,
    SelectionNode,
)

from pyutils.config.providers import YAMLConfigProvider
from pyutils.helpers.errors import BadArgumentsError, BadRequestError, Error
from pyutils.helpers.execution_info import get_execution_id
from pyutils.helpers.general import current_utc_rfc3339


def load_graphql_schema(path: str):
    provider = YAMLConfigProvider(path)
    schema_location = provider.provide(["schema_location"], secret=False)
    return load_schema_from_path(schema_location[0])


schema = load_graphql_schema
query = QueryType()
mutation = MutationType()

# If you need to add a new bindable, such as an enum or scalar definition, you should
# define it and then register that bindable by register_bindables([bindable])
bindables = {query, mutation}

# If you need to add a new middleware, you should define the function
# and then register that middleware by register_middlewares([middleware])
# Critical: Order matters
middlewares = set([])


def register_bindables(new_bindables: Union[list, set]):
    bindables.update(new_bindables)


def register_middlewares(new_middlewares: Union[list, set]):
    middlewares.update(new_middlewares)


def validate_request(request: Any) -> Optional[dict]:
    """
    Validate GraphQL Request, if malformed returns an error dict
    """

    data = None

    try:
        # Forcefully parse as a JSON object for request payload
        if request.content_type.startswith("multipart/form-data"):
            data = combine_multipart_data(
                json.loads(request.form.get("operations")),
                json.loads(request.form.get("map")),
                dict(request.files),
            )
        else:
            data = request.get_json(force=True)
    except Exception:
        pass

    if data is None or not isinstance(data, dict):
        return BadRequestError("Request Body cannot be empty.").extension_details
    variables = data.get("variables")
    if variables and not isinstance(variables, dict):
        return BadRequestError("'variables' key must be a dict.").extension_details


class Context:
    def __init__(self, request=None, **kwargs):
        self._request = request
        for key, val in kwargs.items():
            setattr(self, key, val)

    def update_obj(self, obj: dict):
        protected_properties = ["_request"]
        for key, val in obj.items():
            if key not in protected_properties:
                setattr(self, key, val)

    @property
    def request(self):
        return self._request


def generate_response(graphql_schema: Any, request: Any, **kwargs):
    request_data = None
    try:
        if request.content_type.startswith("multipart/form-data"):
            request_data = combine_multipart_data(
                json.loads(request.form.get("operations")),
                json.loads(request.form.get("map")),
                dict(request.files),
            )
        else:
            request_data = request.get_json()
    except Exception:
        pass

    # MonkeyPatch workaround to be compatible with breaking version of Ariadne >=0.18.0
    #  Use inspect to obtian the params a function takes, since Ariadne package does
    #   not expose __version__ flag which is the package version
    graphql_sync_params = [
        *inspect.getfullargspec(graphql_sync).args,
        *inspect.getfullargspec(graphql_sync).kwonlyargs,
    ]

    MiddlewareManagerClass = MiddlewareManager
    _middlewares = list(middlewares)
    if "middleware_manager_class" in graphql_sync_params:
        # Ariadne  >= 0.18.0
        kwargs["middleware_manager_class"] = MiddlewareManagerClass
        kwargs["middleware"] = _middlewares
    else:
        kwargs["middleware"] = MiddlewareManagerClass(*_middlewares)

    # Note: Passing the request to the context is option. In Flask, the current
    # request is always accessible as flask.request.
    _status, response = graphql_sync(
        graphql_schema,
        data=request_data,
        context_value=Context(request=request),
        debug=False,
        error_formatter=error_formatter,
        **kwargs,
    )
    return response


def is_field_requested(requested_fields: List[dict], field_name: str) -> bool:
    """
    Given a field name, will determine if a field is requested in the query
    Supports fragments and nested fields
    """
    for field in requested_fields:
        if isinstance(field, str):
            continue
        for k, v in field.items():
            if k == "selections":
                if isinstance(v, list):
                    if field_name in v:
                        return True
                    return is_field_requested(v, field_name)
    return False


def error_formatter(error_obj: GraphQLError, debug: bool = False) -> dict:
    if debug:
        # If debug is enabled, reuse Ariadne's formatting logic (not required)
        #   use a separate dict object to track formatted error info which will get
        #   populated in `formatted_err["extensions"]["beppy"]`
        base_extension_key = {}
    else:
        base_extension_key = error_obj.extensions

    default_extension_info = {
        "category": "SUCCESS",
        "code": "OK",
        "errorInstanceId": get_execution_id(),
        "severity": None,
        "timestamp": current_utc_rfc3339(),
    }
    base_extension_key.update(default_extension_info)

    first_err = error_obj.original_error
    if not first_err:
        # GraphQLError
        original_exc = BadArgumentsError(message=error_obj.message)
    else:
        # If first original_error is GraphQLError
        # second original error is actual error
        original_exc = first_err
        if first_err.__class__.__name__ is GraphQLError.__name__:
            original_exc = getattr(first_err, "original_error", None)

    # Check if beppy defined error
    bep_error = isinstance(original_exc, Error)  # BEP Error base class

    # If not, use beppy's default base Error class,
    #   with code being Class Name from Orignal Error
    if not bep_error:
        message = str(original_exc)
        err_class = f"ident::{original_exc.__class__.__name__}"
        original_exc = Error(message=message, code=err_class)

    base_extension_key.update(original_exc.extension_details)

    # Set message to be message from beppy error
    error_obj.formatted["message"] = str(original_exc.message)
    error_obj.message = original_exc.message

    formatted_err: dict = {}
    if debug:
        # If debug is enabled, reuse Ariadne's formatting logic (not required)
        formatted_err = format_error(error_obj, debug)
        if "extensions" not in formatted_err:
            formatted_err["extensions"] = {}
        formatted_err["extensions"]["beppy"] = base_extension_key

        # Add additional details to the GraphQLError Object's extentions
        error_obj.extensions.update(formatted_err.get("extensions", {}))
    else:
        formatted_err = cast(dict, error_obj.formatted)

    if error_obj.locations:
        # This needs to be in format of [{"line": int, "column": int}, ...]
        #   Since this is the format BEP xxSM is suppose to return, otherwise
        #   we will need to create a new key for custom error location tracking for
        #   GraphQL, so we don't break logic of python 'graphql' package

        # GraphQLError.formatted changes:
        #   graphql-core: v1.x.x Did not format the locations field
        #   graphql-core: v3.x.x Started to format the locations field

        # XXX: 'locations' key needs to be overriden with custom output
        try:
            errorLocations = [
                {"line": line, "column": column} for line, column in error_obj.locations
            ]
            formatted_err["locations"] = errorLocations
        except Exception:
            pass

    return formatted_err


def fields_from_selections(
    info: GraphQLResolveInfo, selections: List[SelectionNode]
) -> List[dict]:
    """
    Returns a list of dict with the fields and their selections
    e.g getProductInstances with selection fields such as:

    getProductInstances(input: $getInput) {
      productInstances {
        productInstanceId
        partySummary {
          name
          value
        }
      }
    }

    will return a list of dict like
    [{
        "name": "productInstances",
        "selections": [
            'productInstanceId',
            {"name": "partySummary", "selections": ["name", "value"]}
        ]
    }]
    """

    field_selections: List[dict] = []
    for selection in selections:
        name = ""
        if hasattr(selection, "selection_set") and getattr(
            selection, "selection_set", None
        ):
            name = {"name": selection.name.value, "selections": []}
        if isinstance(selection, FieldNode):
            if hasattr(selection, "selection_set") and getattr(
                selection, "selection_set", None
            ):
                _selections = getattr(selection.selection_set, "selections", None) or []
                selection = fields_from_selections(info, _selections)
                if selection:
                    name["selections"].extend(selection)
            else:
                name = selection.name.value
        elif isinstance(selection, InlineFragmentNode):
            if hasattr(selection, "selection_set") and getattr(
                selection, "selection_set", None
            ):
                _selections = getattr(selection.selection_set, "selections", None) or []
                selection = fields_from_selections(info, _selections)
                if selection:
                    name["selections"].extend(selection)
        elif isinstance(selection, FragmentSpreadNode):
            name = {"name": selection.name.value, "selections": []}
            fragment = info.fragments[selection.name.value]
            name["selections"].extend(
                fields_from_selections(info, fragment.selection_set.selections)
            )
        field_selections.append(name)
    return field_selections


def get_selected_fields(info: GraphQLResolveInfo) -> List[dict]:
    """
    Returns a dict of selected fields and their subfields
    """
    names: List[str] = []
    for node in info.field_nodes:
        if node.selection_set is None:
            continue
        selections = node.selection_set.selections or []
        names.extend(fields_from_selections(info, selections))

    return names
