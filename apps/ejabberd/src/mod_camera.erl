%% ====================================================================================
%% camera module
%% doc here: https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#camera
%%
%% ====================================================================================

-module(mod_camera).

-behaviour(gen_mod).

-export([start/2, stop/1, process_iq/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(NS_CAMERA, <<"aft:camera">>).

-record(camera, {
    id :: binary() | integer(),
    project_id :: binary() | integer(),
    ip :: binary(),
    port :: binary(),
    username :: binary(),
    password :: binary(),
    description :: binary()
}).

start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host,
        ?NS_CAMERA, ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_CAMERA).

%% ================================================
%% iq handler
%% ================================================

process_iq(From, To, #iq{xmlns = ?NS_CAMERA, type = _Type, sub_el = SubEl} = IQ) ->
    case SubEl of
        #xmlel{name = <<"query">>} ->
            case xml:get_tag_attr_s(<<"query_type">>, SubEl) of
                <<"list">> ->
                    list_camera(From, To, IQ);
                _ ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
            end;
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;

process_iq(_, _, IQ) ->
    #iq{sub_el = SubEl} = IQ,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


%% @doc get camera list by project id
%% https://github.com/ZekeLu/MongooseIM/wiki/Extending-XMPP#camera
list_camera(#jid{lserver = LServer} = _From, _To, #iq{sub_el = SubEl} = IQ) ->
    ProjectId = xml:get_tag_attr_s(<<"project">>, SubEl),
    CameraList = get_camera_list(LServer, ProjectId),
    IQ#iq{type = result, sub_el = [SubEl#xmlel{children = [{xmlcdata, CameraList}]}]}.

%% =====================================
%% odbc
%% ====================================

get_camera_list(LServer, ProjectId) ->
    Query = [<<"select id,ip,port,username,password,description from camera where project_id=">>, ProjectId],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, R} ->
            JsonArray = [{struct, record_to_json(#camera{
                id = Id,
                project_id = ProjectId,
                ip = Ip,
                port = Port,
                username = UserName,
                password = Password,
                description = Description
            })} || {Id, Ip, Port, UserName, Password, Description} <- R],
            iolist_to_binary(mochijson2:encode(JsonArray))
    end.

record_to_json(Camera) ->
    [_ | L] = tuple_to_list(Camera),
    lists:filter(fun(X) ->
        case X of
            {_, undefined} -> false;
            {_, null} -> false;
            _ -> true
        end
    end,
        lists:zip(record_info(fields, camera), L)
    ).