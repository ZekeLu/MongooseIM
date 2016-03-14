-module(mod_project_library).

-behaviour(gen_mod).

%% iq, hooks exports
-export([start/2, stop/1, process_iq/3,
    create_project/7, delete_member/4, add_member/3]).


%% Internal exports
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/2, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-include("jlib.hrl").
-include("ejabberd.hrl").
-include("mod_mms.hrl").

-define(TYPE_ADMIN_MANAGER, <<"-3">>).
-define(TYPE_SHARE,         <<"-2">>).
-define(TYPE_ROOT,          <<"-1">>).
-define(TYPE_PUBLIC,        <<"0">>).  % public folder.
-define(TYPE_PUB_SUB,       <<"1">>).  % public sub-folder.
-define(TYPE_PERSON,        <<"2">>).  % personal folder.
%-define(TYPE_PER_PUBLIC,    <<"3">>).  % personal public sub-folder.
-define(TYPE_PER_SHARE,     <<"4">>).  % personal share sub-folder.
-define(TYPE_PER_PRIVATE,   <<"5">>).  % personal private sub-folder.
-define(TYPE_DEPARTMENT,    <<"6">>).  % department folder.
-define(TYPE_DEP_SUB,       <<"7">>).  % department sub-folder.

-define(LOG_ADD_FILE,           <<"0">>).
-define(LOG_ADD_FOLDER,         <<"1">>).
-define(LOG_DELETE_FILE,        <<"2">>).
-define(LOG_DELETE_FOLDER,      <<"3">>).
-define(LOG_MOVE_FILE,          <<"4">>).
-define(LOG_MOVE_FOLDER,        <<"5">>).
-define(LOG_RENAME,             <<"6">>).
-define(LOG_ADD_FILE_VERSION,   <<"7">>).
-define(LOG_RECOVER_FILE,       <<"8">>).

-define(FILE_PAGE_ITEM_COUNT, <<"10">>).

start(Host, _Opts) ->
    start_worker(Host),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_AFT_LIBRARY, ?MODULE, process_iq, no_queue),
    ejabberd_hooks:add(create_project, Host, ?MODULE, create_project, 50),
    ejabberd_hooks:add(delete_member, Host, ?MODULE, delete_member, 50),
    ejabberd_hooks:add(add_member, Host, ?MODULE, add_member, 50),
    ok.

stop(Host) ->
    ejabberd_hooks:delete(add_member, Host, ?MODULE, add_member, 50),
    ejabberd_hooks:delete(delete_member, Host, ?MODULE, delete_member, 50),
    ejabberd_hooks:delete(create_project, Host, ?MODULE, create_project, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_AFT_LIBRARY),
    stop_worker(Host),
    ok.

%% ------------------------------------------------------------------
%% higher function. called by process_iq.
%% ------------------------------------------------------------------

process_iq(From, To, #iq{xmlns = ?NS_AFT_LIBRARY, type = _Type, sub_el = SubEl} = IQ) ->
    case xml:get_tag_attr_s(<<"subtype">>, SubEl) of
        <<"list_folder">> ->
            list_folder(From, To, IQ);
        <<"add_folder">> ->
            add(From, To, IQ, <<"folder">>);
        <<"add_file">> ->
            add(From, To, IQ, <<"file">>);
        <<"delete_folder">> ->
            delete(From, To, IQ, <<"folder">>);
        <<"delete_file">> ->
            delete(From, To, IQ, <<"file">>);
        <<"rename_folder">> ->
            rename(From, To, IQ, <<"folder">>);
        <<"rename_file">> ->
           rename(From, To, IQ, <<"file">>);
        <<"share">> ->
            share(From, To, IQ);
        <<"move_folder">> ->
            move(From, To, IQ, <<"folder">>);
        <<"move_file">> ->
            move(From, To, IQ, <<"file">>);
        <<"add_version">> ->
            add_version(From, To, IQ);
        <<"list_share_users">> ->
            list_share_users(From, To, IQ);
        <<"list_version">> ->
            list_version(From, To, IQ);
        <<"download">> ->
            download(From, To, IQ);
        <<"get_log">> ->
            get_log(From, To, IQ);
        <<"get_trash">> ->
            get_trash(From, To, IQ);
        <<"clear_trash">> ->
            clear_trash(From, To, IQ);
        <<"recover_file">> ->
            recover_file(From, To, IQ);
        <<"get_normal_uuid">> ->
            get_normal_uuid(From, To, IQ);
        <<"search_file">> ->
            search_file(From, To, IQ);
        _ ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}
    end;
process_iq(_, _, IQ) ->
    #iq{sub_el = SubEl} = IQ,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_BAD_REQUEST]}.


%% ------------------------------------------------------------------
%% hooks
%% ------------------------------------------------------------------

delete_member(LServer, DeleteJID, Project, Job) ->
    clear_trash_ex(LServer, DeleteJID, Project),

    %% update department folder's owner.
    case ejabberd_odbc:sql_query(LServer, ["select o1.id, o1.department_id from organization as o1, organization as o2 where o1.project='", Project, "' and o2.project='", Project,
        "' and o2.id='", Job, "' and o1.department_id =o2.department_id order by o1.lft limit 1;"]) of
        {selected, _, [{Job, Department_ID}]} ->
            case ejabberd_odbc:sql_query(LServer, ["select count(o.id) from organization as o, organization_user as ou where o.project='", Project, "' and o.id='", Job,
                "' and o.id=ou.organization"]) of
                {selected, _, [{<<"0">>}]} ->
                    {updated, _} = ejabberd_odbc:sql_query(LServer, ["update folder set owner='admin' where project='", Project, "' and department_id='", Department_ID, "'"] );
                _ ->
                    ok
            end;
        _ -> ok
    end.


add_member(LServer, Project, AddIDJIDList) ->
    InJIDs =
    lists:foldl(fun({_, E_JID}, AccIn) ->
            AccIn1 = case AccIn of
                         <<>> -> <<>>;
                         _ -> <<AccIn/binary, ",">>
                     end,
            <<AccIn1/binary, "'", E_JID/binary, "'">>
        end,
        <<>>,
        AddIDJIDList),

    {selected, _, PersonalFolderExistJIDList} = ejabberd_odbc:sql_query(LServer, ["select owner from folder where project='",
        Project, "' and type='", ?TYPE_PERSON, "' and owner in(", InJIDs, ")"]),
    NewIDJID = lists:filter(fun({_, E_JID}) ->
            case lists:keyfind(E_JID, 1, PersonalFolderExistJIDList) of
                false -> true;
                _ -> false
            end
        end,
        AddIDJIDList),

    Init = <<"insert into folder(type, name, creator, owner, parent, project, status, department_id) values">>,
    Query = lists:foldl(fun({_, JID}, AccIn) ->
        SJID = escape(JID),
        AccIn1 = if
                     AccIn =:= Init -> Init;
                     true -> <<AccIn/binary, ",">>
                 end,
        <<AccIn1/binary, "('", ?TYPE_PERSON/binary, "', '', '", SJID/binary, "', '", SJID/binary, "', '-1', '", Project/binary, "', '1', '-1')">>
    end,
        Init,
        NewIDJID),

    Length = lists:flatlength(NewIDJID),
    if
        Length > 0 ->
            case ejabberd_odbc:sql_query(LServer, Query) of
                {updated, _} -> ok;
                _ -> ?ERROR_MSG("[ERROR]:create personal folder where add member failed for project ~p", [Project])
            end;
        true ->
            ok
    end,

    %% update department folder's owner.
    {selected, _, DepartmentLeaderJobs} = ejabberd_odbc:sql_query(LServer, ["select id, department_id from (select id, department_id from organization ",
        " where project='", Project, "' order by lft ) as o group by department_id"]),
    lists:foreach(fun({E_ID, _E_JID}) ->
        case lists:keyfind(E_ID, 1, DepartmentLeaderJobs) of
            false -> ok;
            {_, E_DepartmentID} ->
                ejabberd_odbc:sql_query(LServer, ["update folder set owner='", E_ID, "' where project='", Project, "' and department_id='", E_DepartmentID, "';" ])
        end
        end,
        AddIDJIDList).



create_project(LServer, BareJID, DepartmentID, TemplateId, TemplateJob, Project, Job) ->
    Negative_TemplateId = integer_to_binary( 0 - binary_to_integer(TemplateId)),
    %% templateJob is department leader in template menu Job is department leader in project.
    IsLeader = is_department_leader_Job(LServer, Negative_TemplateId, DepartmentID, TemplateJob),

    F = fun() ->
        {updated, _} = ejabberd_odbc:sql_query_t(["insert into folder(type, name, creator, owner, parent, project, status, department_id)",
            " select type, name, creator, owner, parent, ", Project, ", status, department_id from folder where project='", Negative_TemplateId, "';"]),
        if
            IsLeader =:= true ->
                {updated, 1} = ejabberd_odbc:sql_query_t(["update folder set owner='", Job, "' where project='", Project,
                    "' and type='", ?TYPE_DEPARTMENT, "' and department_id='", DepartmentID, "'; "]);
            true -> ok
        end,
        {updated, 1} = ejabberd_odbc:sql_query_t(["insert into folder(type, name, creator, owner, parent, project, status, department_id) values('", ?TYPE_PERSON, "','', '",
            escape(BareJID), "', '", escape(BareJID), "', '-1', '", Project, "', '1', '-1');"]),
        ok
    end,

    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            ok;
        _ ->
            ?ERROR_MSG("[ERROR]:create folder failed for project ~p where create project", [Project])
    end.

%% ------------------------------------------------------------------
%% higher function. called by process_iq.
%% ------------------------------------------------------------------

list_folder(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Folder} = lists:keyfind(<<"folder">>, 1, Data),

    case list_folder_ex(S, BareJID, Folder, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_folder(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

add(From, _To, #iq{type = set, sub_el = SubEl} = IQ, Type) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Parent} = lists:keyfind(<<"parent">>, 1, Data),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),

    Return = case Type of
                 <<"folder">> ->
                     add_folder_ex(S, BareJID, Parent, Name, Project);
                 <<"file">> ->
                     {_, UUID} = lists:keyfind(<<"uuid">>, 1, Data),
                     {_, Size} = lists:keyfind(<<"size">>, 1, Data),
                     File = find_key_value(Data, <<"file">>),
                     add_file_ex(S, BareJID, Parent, Name, UUID, Size, Project, File)
             end,
    case Return of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
add(_, _, IQ, _) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

delete(From, _To, #iq{type = set, sub_el = SubEl} = IQ, Type) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),

    Return = case Type of
                <<"folder">> -> delete_folder_ex(S, BareJID, ID, Project);
                <<"file">> ->   delete_file_ex(S, BareJID, ID, Project)
             end,
    case Return of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
delete(_, _, IQ, _) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

rename(From, _To, #iq{type = set, sub_el = SubEl} = IQ, Type) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),

    Return = rename_file_or_folder_ex(S, BareJID, ID, Name, Project, Type),
    case Return of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
rename(_, _, IQ, _) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

share(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, Users} = lists:keyfind(<<"users">>, 1, Data),

    case share_ex(S, BareJID, ID, Users, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            IQ#iq{type = result}
    end;
share(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

move(From, _To, #iq{type = set, sub_el = SubEl} = IQ, Type) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, DestFolder} = lists:keyfind(<<"dest_parent">>, 1, Data),

    Return = move_ex(S, BareJID, ID, DestFolder, Project, Type),
    case Return of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            IQ#iq{type = result}
    end;
move(_, _, IQ, _) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

add_version(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, UUID} = lists:keyfind(<<"uuid">>, 1, Data),
    {_, Size} = lists:keyfind(<<"size">>, 1, Data),

    case add_version_ex(S, BareJID, ID, UUID, Size, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
add_version(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

list_share_users(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),

    case list_share_users_ex(S, BareJID, ID, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_share_users(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

list_version(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),

    case list_version_ex(S, BareJID, ID, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
list_version(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

download(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, UUID} = lists:keyfind(<<"uuid">>, 1, Data),

    case download_ex(S, BareJID, ID, UUID, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
download(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


get_log(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    Before = case lists:keyfind(<<"before">>, 1, Data) of
                {_, T1} -> T1;
                false -> false
            end,
    After = case lists:keyfind(<<"after">>, 1, Data) of
                {_, T2} -> T2;
                false -> false
            end,
    Count = case lists:keyfind(<<"count">>, 1, Data) of
                {_, T3} -> T3;
                false -> false
            end,

    case check_log_trash_parameter(Before, After, Count) of
        error ->
            IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]};
        {B, A, C} ->
            case get_log_ex(S, BareJID, B, A, C, Project) of
                {error, Error} ->
                    IQ#iq{type = error, sub_el = [SubEl, Error]};
                {ok, Result} ->
                    IQ#iq{type = result,
                        sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
            end
    end;
get_log(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

get_trash(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    Before = case lists:keyfind(<<"before">>, 1, Data) of
                 {_, T1} -> T1;
                 false -> false
             end,
    After = case lists:keyfind(<<"after">>, 1, Data) of
                {_, T2} -> T2;
                false -> false
            end,
    Count = case lists:keyfind(<<"count">>, 1, Data) of
                {_, T3} -> T3;
                false -> false
            end,
    case check_log_trash_parameter(Before, After, Count) of
        error ->
            IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]};
        {B, A, C} ->
            case get_trash_ex(S, BareJID, B, A, C, Project) of
                {error, Error} ->
                    IQ#iq{type = error, sub_el = [SubEl, Error]};
                {ok, Result} ->
                    IQ#iq{type = result,
                        sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
            end
    end;
get_trash(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

clear_trash(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),

    case clear_trash_ex(S, BareJID, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            %% TOFIX: delete file uuid in mms_file table.
            IQ#iq{type = result}
    end;
clear_trash(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

recover_file(From, _To, #iq{type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),
    {_, DestFolder} = lists:keyfind(<<"dest_parent">>, 1, Data),

    case recover_file_ex(S, BareJID, ID, Name, DestFolder, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        ok ->
            IQ#iq{type = result}
    end;
recover_file(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


get_normal_uuid(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    IDList = mochijson2:decode(xml:get_tag_cdata(SubEl)),

    case get_normal_uuid_ex(S, BareJID, IDList, Project) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;
get_normal_uuid(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

search_file(From, _To, #iq{type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BareJID = <<U/binary, "@", S/binary>>,
    Project = xml:get_tag_attr_s(<<"project">>, SubEl),
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Folder} = lists:keyfind(<<"folder">>, 1, Data),
    {_, Page} = lists:keyfind(<<"page">>, 1, Data),
    {_, Name} = lists:keyfind(<<"name">>, 1, Data),
    Count = case lists:keyfind(<<"count">>, 1, Data) of
                {_, <<>>} ->
                    ?FILE_PAGE_ITEM_COUNT;
                {_, Value} ->
                    integer_to_binary(binary_to_integer(Value, true, 1, 50));
                false ->
                    ?FILE_PAGE_ITEM_COUNT
            end,
    case search_file_ex(S, BareJID, Project, Folder, Name, Page, Count) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{children = [{xmlcdata, Result}]}]}
    end;

search_file(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

%% ------------------------------------------------------------------
%% bridge betwwen odbc and higer function.
%% ------------------------------------------------------------------

list_folder_ex(LServer, BareJID, Folder, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, JobID, AdminJID} ->
            Querys = if
                         (Folder =:= <<>>) or (Folder =:= <<"-1">>) ->
                             folder_sql_query(LServer, ?TYPE_ROOT, BareJID, JobID, Folder, Project, false, BareJID =:= AdminJID);
                         (Folder =:= <<"-2">>) ->
                            folder_sql_query(LServer, ?TYPE_SHARE, BareJID, JobID, Folder, Project, false, BareJID =:= AdminJID);
                         (Folder =:= <<"-3">>) ->
                             folder_sql_query(LServer, ?TYPE_ADMIN_MANAGER, BareJID, JobID, Folder, Project, false, BareJID =:= AdminJID);
                         true ->
                             case query_folder_info('type-owner', LServer, ["and id='", Folder, "';"]) of
                                 [{Type, Owner}] ->
                                     folder_sql_query(LServer, Type, BareJID, JobID, Folder, Project, Owner =:= BareJID, BareJID =:= AdminJID);
                                 _ ->
                                     {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                             end
                     end,

            case Querys of
                {error, ErrorReason} -> {error, ErrorReason};
                _ ->
                    {FileAcc, FolderAcc} =
                    lists:foldl(fun({file, Query}, {FileAccIn, FolderAccIn}) ->
                        case ejabberd_odbc:sql_query(LServer, Query) of
                            {selected, _, R} ->
                                NewFileAcc = lists:foldl(fun(Item, AccIn) -> [Item | AccIn] end, FileAccIn, R),
                               {NewFileAcc, FolderAccIn};
                            _ ->
                                {FileAccIn, FolderAccIn}
                        end;
                        ({folder, Query}, {FileAccIn, FolderAccIn}) ->
                            case ejabberd_odbc:sql_query(LServer, Query) of
                                {selected, _, R} ->
                                    NewFolderAcc = lists:foldl(fun(Item, AccIn) -> [Item | AccIn] end, FolderAccIn, R),
                                    {FileAccIn, NewFolderAcc};
                                _ ->
                                    {FileAccIn, FolderAccIn}
                            end
                        end,
                    {[], []},
                    Querys),
                    FileJson = build_file_result(FileAcc),
                    FolderJson = build_folder_result(FolderAcc),
                    {ok, <<"{\"parent\":\"", Folder/binary, "\", \"folder\":", FolderJson/binary, ", \"file\":", FileJson/binary, "}">>}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

add_folder_ex(LServer, BareJID, Parent, Name, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            SBareJID = escape(BareJID),
            case query_folder_info('id-name-type-owner-department_id', LServer, ["and project='", Project, "' and id='", Parent, "';"]) of
                [{ParentID, ParentName, ParentType, ParentOwner, ParentPartID}] ->
                    case create_folder_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) of
                        {allowed, SelfType} ->
                            F = fun() ->
                                case ejabberd_odbc:sql_query_t(["select count(id) from folder where parent='", ParentID, "' and name='", escape(Name), "' and status='1';"]) of
                                    {selected,_,[{<<"0">>}]} ->
                                        {updated, 1} = ejabberd_odbc:sql_query_t(["insert into folder(type, name, creator, owner, parent, project, department_Id) values('",
                                            SelfType, "', '", escape(Name), "', '", SBareJID, "', '", escape(ParentOwner), "', '", ParentID, "', '", Project, "', '", ParentPartID, "');"]),
                                        {selected, _, InsertFolder} = ejabberd_odbc:sql_query_t([query_folder_column(normal), "and id=last_insert_id();"]),
                                        {ok, InsertFolder};
                                    _ ->
                                        folder_exist
                                end
                            end,
                            case ejabberd_odbc:sql_transaction(LServer, F) of
                                {atomic, {ok, InsertFolder}} ->
                                    if
                                        ParentType =:= ?TYPE_PUBLIC ->
                                            store_log(LServer, BareJID, ?LOG_ADD_FOLDER, <<ParentName/binary, ">", Name/binary>>, "",  Project);
                                        true ->
                                            nothing_to_do
                                    end,
                                    FolderJson = build_folder_result(InsertFolder),
                                    {ok, <<"[{\"parent\":\"", ParentID/binary, "\", \"folder\":", FolderJson/binary, "}]">>};
                                {atomic, folder_exist} ->
                                    {error, ?AFT_ERR_FOLDER_EXIST};
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                not_exist ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member,finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

add_file_ex(LServer, BareJID, Parent, Name, UUID, Size, Project, FileID) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            case check_add_file_uuid_valid(LServer, BareJID, UUID, FileID) of
                invalid -> {error, ?AFT_ERR_INVALID_FILE_ID};
                {valid, Type, NewUUID} ->
                    SBareJID = escape(BareJID),
                    case query_folder_info('id-name-type-owner', LServer, ["and project='", Project, "' and id='", Parent, "';"]) of
                        [{ParentID, ParentName, ParentType, ParentOwner}] ->
                            case create_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) of
                                allowed ->
                                    F = fun() ->
                                        case ejabberd_odbc:sql_query_t(["select count(id) from file where folder='", ParentID, "' and name='", escape(Name), "' and status='1';"]) of
                                            {selected,_,[{<<"0">>}]} ->
                                                SUUID = case Type of <<"3">> -> escape(UUID); <<"2">> -> escape(NewUUID) end,
                                                {updated, 1} = ejabberd_odbc:sql_query_t(["insert into file(uuid, name, size_byte, creator, version_count, folder) values('",
                                                    SUUID, "', '", escape(Name), "', '", Size, "', '", SBareJID, "', '1', '", ParentID, "');"]),
                                                {selected, _, InsertFile} = ejabberd_odbc:sql_query_t([query_file_column(normal), "and id=last_insert_id();"]),
                                                {ok, InsertFile};
                                            _ ->
                                                file_exist
                                        end
                                    end,
                                    case ejabberd_odbc:sql_transaction(LServer, F) of
                                        {atomic, {ok, InsertFile}} ->
                                            if
                                                ParentType =:= ?TYPE_PUBLIC ->
                                                    store_log(LServer, BareJID, ?LOG_ADD_FILE, Name, <<ParentName/binary, ">">>,  Project);
                                                ParentType =:= ?TYPE_PUB_SUB ->
                                                    [{PPName}] = query_parent_info(name, LServer, ParentID, <<"folder">>),
                                                    store_log(LServer, BareJID, ?LOG_ADD_FILE, Name, <<PPName/binary, ">", ParentName/binary, ">" >>,  Project);
                                                true ->
                                                    nothing_to_do
                                            end,
                                            FileJson = build_file_result(InsertFile),
                                            case Type of
                                                <<"3">> ->
                                                    case FileID of
                                                        false ->
                                                            {ok, <<"[{\"parent\":\"", ParentID/binary, "\", \"file\":", FileJson/binary, "}]">>};
                                                        _ ->
                                                            {ok, <<"[{\"src_lib_uuid\":\"", UUID/binary, "\", \"new_lib_uuid\":\"", NewUUID/binary,
                                                            "\"}, {\"parent\":\"", ParentID/binary, "\", \"file\":", FileJson/binary, "}]">>}
                                                    end;
                                                <<"2">> -> {ok, <<"[{\"normal_uuid\":\"", UUID/binary, "\", \"lib_uuid\":\"", NewUUID/binary,
                                                    "\"}, {\"parent\":\"", ParentID/binary, "\", \"file\":", FileJson/binary, "}]">>}
                                            end;
                                        {atomic, file_exist} ->
                                            {error, ?AFT_ERR_FILE_EXIST};
                                        _ ->
                                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                                    end;
                                not_allowed ->
                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                            end;
                        not_exist ->
                            {error, ?AFT_ERR_ALLREADY_FINISHED}
                    end
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

delete_folder_ex(LServer, BareJID, ID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            case query_self_parent_info('type-name#id-name-type-owner', LServer, ID, <<"folder">>) of
                [{SelfType, SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    case delete_folder_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) of
                        allowed ->
                            {ok, Location} =  get_folder_dir(LServer, ParentID, ParentType, ParentName),
                            DeleteTime = now_to_microseconds(erlang:now()),
                            F = fun() ->
                                {updated, _} = ejabberd_odbc:sql_query_t(["update file set status='0', location='", escape(<<Location/binary, ">", SelfName/binary>>),
                                    "', deleted_at='", integer_to_binary(DeleteTime), "' where folder='", ID, "'"]),
                                if
                                    SelfType =:= ?TYPE_PER_SHARE ->
                                        {updated, _} = ejabberd_odbc:sql_query_t(["delete from share_users where folder='", ID, "';"]);
                                    true ->
                                        nothing_to_do
                                end,
                                case ejabberd_odbc:sql_query_t(["update folder set status='0' where id='", ID, "';"]) of
                                    {updated, 1} ->
                                        ok;
                                    {updated, 0} ->
                                        not_exist;
                                    _ ->
                                        error
                                end
                            end,
                            case ejabberd_odbc:sql_transaction(LServer, F) of
                                {atomic, ok} ->
                                    if
                                        ParentType =:= ?TYPE_PUBLIC ->
                                            store_log(LServer, BareJID, ?LOG_DELETE_FOLDER, <<ParentName/binary, ">", SelfName/binary>>, "",  Project);
                                        true ->
                                            nothing_to_do
                                    end,
                                    {ok, <<"{\"id\":\"", ID/binary, "\"}">>};
                                {atomic, not_exist} ->
                                    {error, ?AFT_ERR_FOLDER_NOT_EXIST};
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

delete_file_ex(LServer, BareJID, ID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            case query_self_parent_info('name#id-name-type-owner', LServer, ID, <<"file">>) of
                [{SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    case delete_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) of
                        allowed ->
                            {ok, Location} = get_file_dir(LServer, ParentID, ParentType, ParentName),
                            DeleteTime = now_to_microseconds(erlang:now()),
                             case ejabberd_odbc:sql_query(LServer, ["update file set status='0', location='", escape(Location),
                                "' , deleted_at='", integer_to_binary(DeleteTime), "' where id='", ID, "';"]) of
                                    {updated, 1} ->
                                        if
                                            ParentType =:= ?TYPE_PUBLIC ->
                                                store_log(LServer, BareJID, ?LOG_DELETE_FILE, <<ParentName/binary, ">", SelfName/binary>>, "",  Project);
                                            ParentType =:= ?TYPE_PUB_SUB ->
                                                store_log(LServer, BareJID, ?LOG_DELETE_FILE, <<Location/binary, ">", SelfName/binary>>, "", Project);
                                            true ->
                                                nothing_to_do
                                        end,
                                        {ok, <<"{\"id\":\"", ID/binary, "\"}">>};
                                    {updated, 0} ->
                                        {error, ?AFT_ERR_FILE_NOT_EXIST};
                                    _ ->
                                        {error, ?ERR_INTERNAL_SERVER_ERROR}
                                end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                    _ ->
                        {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

rename_file_or_folder_ex(LServer, BareJID, ID, Name, Project, Type) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            case query_self_parent_info('creator-name#id-name-type-owner', LServer, ID, Type) of
                [{SelfUpdater, SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    IsAllowed = case Type  of
                                    <<"folder">> ->
                                        rename_folder_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, SelfUpdater);
                                    <<"file">> ->
                                        rename_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, SelfUpdater)
                                end,
                    case IsAllowed of
                        allowed ->
                            Query = case Type  of
                                        <<"folder">> ->
                                            ["select count(id) from file where folder='", ParentID, "' and name='", escape(Name), "' and status='1';"];
                                        <<"file">> ->
                                            ["select count(id) from folder where parent='", ParentID, "' and name='", escape(Name), "' and status='1';"]
                                    end,
                            F = fun() ->
                                case ejabberd_odbc:sql_query_t(Query) of
                                    {selected,_,[{<<"0">>}]} ->
                                        case ejabberd_odbc:sql_query_t(["update ", Type ," set name='", escape(Name), "' where id='", ID, "';"]) of
                                            {updated, 1} -> ok;
                                            _ -> error
                                        end;
                                    {selected,_,[{<<"1">>}]} ->
                                        exist;
                                    _ ->
                                        error
                                end
                            end,

                            case ejabberd_odbc:sql_transaction(LServer, F) of
                                {atomic, ok} ->
                                    if
                                        ParentType =:= ?TYPE_PUBLIC ->
                                            store_log(LServer, BareJID, ?LOG_RENAME, <<ParentName/binary, ">", SelfName/binary>>,
                                                <<ParentName/binary, ">", Name/binary>>,  Project);
                                        ParentType =:= ?TYPE_PUB_SUB ->
                                            [{PPName}] = query_parent_info(name, LServer, ParentID, <<"folder">>),
                                            store_log(LServer, BareJID, ?LOG_RENAME, <<PPName/binary, ">", ParentName/binary, ">", SelfName/binary>>,
                                                <<PPName/binary, ">", ParentName/binary, ">", Name/binary>>, Project);
                                        true ->
                                            nothing_to_do
                                    end,
                                    {ok, <<"{\"id\":\"", ID/binary, "\", \"name\":\"", Name/binary, "\"}">>};
                                {atomic, exist} ->
                                    case Type of
                                        <<"folder">> -> {error, ?AFT_ERR_FOLDER_EXIST};
                                        <<"file">> -> {error, ?AFT_ERR_FILE_EXIST}
                                    end;
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

share_ex(LServer, BareJID, ID, Users, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            case query_self_parent_info('type#id-type-owner', LServer, ID, <<"folder">>) of
                [{SelfType, ParentID, ParentType, ParentOwner}] ->
                    case share_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, SelfType) of
                        allowed ->
                            AfterType = if
                                            is_list(Users) ->
                                                if
                                                    length(Users) > 0 -> ?TYPE_PER_SHARE;
                                                    true -> ?TYPE_PER_PRIVATE
                                                end;
                                            true -> error
                                        end,
                            case AfterType of
                                    error -> {error, ?ERR_BAD_REQUEST};
                                _ ->
                                    InsertValues = if
                                               AfterType =:= ?TYPE_PER_SHARE ->
                                                   lists:foldl(fun(E_JID, AccIn) ->
                                                       AccIn1 = if
                                                                    AccIn =:= <<>> -> <<>>;
                                                                    true -> <<AccIn/binary, ",">>
                                                                end,
                                                       <<AccIn1/binary, "('", ID/binary, "', '", E_JID/binary, "')">>
                                                   end,
                                                       <<>>,
                                                       Users);
                                               true ->
                                                   <<>>
                                           end,

                                    InsertQuery = if
                                                      InsertValues =:= <<>> -> <<>>;
                                                      true -> <<"insert into share_users(folder, userjid) values", InsertValues/binary, ";">>
                                                  end,

                                    F = fun() ->
                                        if
                                            (SelfType /= AfterType) ->
                                                {updated, 1} = ejabberd_odbc:sql_query_t(["update folder set type='", AfterType, "' where id='", ID, "';"]);
                                            true ->
                                                nothing_to_do
                                        end,
                                        if
                                            (SelfType =:= ?TYPE_PER_SHARE) ->
                                                {updated, _} = ejabberd_odbc:sql_query_t(["delete from share_users where folder='", ID, "';"]);
                                            true -> nothing_to_do

                                        end,
                                        if
                                            (AfterType =:= ?TYPE_PER_SHARE) and (InsertQuery /= <<>>) ->
                                                {updated, _} = ejabberd_odbc:sql_query_t(InsertQuery),
                                                ok;
                                            true ->
                                                ok
                                        end
                                    end,
                                    case ejabberd_odbc:sql_transaction(LServer, F) of
                                        {atomic, ok} ->
                                            ok;
                                        _ ->
                                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                                    end

                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

move_ex(LServer, BareJID, ID, DestFolder, Project, Type) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            case query_self_parent_info('name#id-name-type-owner', LServer, ID, Type) of
                [{SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    case query_folder_info('id-name-type-owner', LServer, ["and id='", DestFolder, "';"]) of
                        [{DestID, DestName, DestType, DestOwner}] ->
                            case move_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID,
                                ParentType, ParentOwner, Type, 0, DestFolder, DestType, DestOwner) of
                                allowed ->
                                    %% TOFIX: shoule to update folder(and inner file) or file creator=BareJID ???
                                    F = fun() ->
                                        case Type of
                                            <<"file">> ->
                                                case ejabberd_odbc:sql_query_t(["select count(id) from file where folder='", DestFolder, "' and name='", escape(SelfName), "' and status='1';"]) of
                                                    {selected,_,[{<<"0">>}]} ->
                                                        {updated, 1} = ejabberd_odbc:sql_query_t(["update file set folder='", DestFolder, "' where id='", ID, "';"]),
                                                        ok;
                                                    _ ->
                                                        file_exist
                                                end;
                                            <<"folder">> ->
                                                case ejabberd_odbc:sql_query_t(["select count(id) from folder where parent='", DestFolder, "' and name='", escape(SelfName), "' and status='1';"]) of
                                                    {selected,_,[{<<"0">>}]} ->
                                                        TypeAfterMoved = case DestType of
                                                                             ?TYPE_PUBLIC -> ?TYPE_PUB_SUB;
                                                                             ?TYPE_DEPARTMENT -> ?TYPE_DEP_SUB
                                                                         end,
                                                        {updated, 1} = ejabberd_odbc:sql_query_t(["update folder set parent='", DestFolder, "', owner='", DestOwner, "', type='", TypeAfterMoved, "' where id='", ID, "';"]),
                                                        ok;
                                                    _ ->
                                                        folder_exist
                                                end
                                        end
                                    end,
                                    case ejabberd_odbc:sql_transaction(LServer, F) of
                                        {atomic, ok} ->
                                            Text = if
                                                       (ParentType =:= ?TYPE_PUBLIC) ->
                                                           <<ParentName/binary, ">", SelfName/binary>>;
                                                       (ParentType =:= ?TYPE_PUB_SUB) ->
                                                           [{PPName1}] = query_parent_info(name, LServer, ParentID, <<"folder">>),
                                                           <<PPName1/binary, ">", ParentName/binary, ">", SelfName/binary>>;
                                                       (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
                                                           <<SelfName/binary>>;
                                                       true ->
                                                           false
                                                   end,

                                            Path = if
                                                       (DestType =:= ?TYPE_PUBLIC) ->
                                                           <<DestName/binary, ">">>;
                                                       (DestType =:= ?TYPE_PUB_SUB) ->
                                                           [{PPName2}] = query_parent_info(name, LServer, DestID, <<"folder">>),
                                                           <<PPName2/binary, ">", DestName/binary, ">">>;
                                                       true ->
                                                           false
                                                   end,
                                            Operation = if
                                                            Type =:= <<"folder">> -> ?LOG_MOVE_FOLDER;
                                                            true -> ?LOG_MOVE_FILE
                                                        end,
                                            if
                                                (Text /= false) and (Path /= false) ->
                                                    store_log(LServer, BareJID, Operation, Text, Path, Project);
                                                true ->
                                                    nothing_to_do
                                            end,
                                            ok;
                                        {atomic, file_exist} ->
                                            {error, ?AFT_ERR_FILE_EXIST};
                                        {atomic, folder_exist} ->
                                            {error, ?AFT_ERR_FOLDER_EXIST};
                                        _ ->
                                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                                    end;
                                not_allowed ->
                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                            end;
                        _ ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

add_version_ex(LServer, BareJID, ID, UUID, Size, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, running, Job, AdminJID} ->
            case query_self_parent_info('id-name-version_count#id-type-name-owner', LServer, ID, <<"file">>) of
                [{_SelfID, SelfName, VersionCount, ParentID, ParentType, ParentName, ParentOwner}] -> %% _SelfID, just only check file is exist or not, can check id is valied or not.
                    case add_version_privilige(LServer, Project, BareJID, Job, AdminJID,  ParentID, ParentType, ParentOwner) of
                        allowed ->
                            F = fun() ->
                                {updated, 1} = ejabberd_odbc:sql_query_t(["insert into file_version(file, uuid, size_byte, creator, created_at) ",
                                    " select id, uuid, size_byte, creator, created_at from file where id='", ID, "';"]),
                                NewVersionCount = integer_to_binary(binary_to_integer(VersionCount) + 1),
                                {selected, [<<"time">>], [{Time}]} = ejabberd_odbc:sql_query_t(["select current_timestamp() as time;"]),
                                {updated, 1} = ejabberd_odbc:sql_query_t(["update file set uuid='", UUID, "', size_byte='", Size,  "', creator='", escape(BareJID),
                                            "', version_count='", NewVersionCount, "', created_at='", Time, "' where id='", ID, "';"]),
                                {ok, [{ID, UUID, SelfName, Size, BareJID, NewVersionCount, ParentID, Time}]}
                            end,
                            case ejabberd_odbc:sql_transaction(LServer, F) of
                                {atomic, {ok, FileInfo}} ->
                                    if
                                        ParentType =:= ?TYPE_PUBLIC ->
                                            store_log(LServer, BareJID, ?LOG_ADD_FILE_VERSION, <<ParentName/binary, ">", SelfName/binary>>,
                                                "",  Project);
                                        ParentType =:= ?TYPE_PUB_SUB ->
                                            [{PPName}] = query_parent_info(name, LServer, ParentID, <<"folder">>),
                                            store_log(LServer, BareJID, ?LOG_ADD_FILE_VERSION, <<PPName/binary, ">", ParentName/binary, ">", SelfName/binary>>, "", Project);
                                        true ->
                                            nothing_to_do
                                    end,
                                    FileJson = build_file_result(FileInfo),
                                    {ok, <<"{\"parent\":\"", ParentID/binary, "\", \"file\":", FileJson/binary, "}">>};
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

list_share_users_ex(LServer, BareJID, ID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            case query_folder_info('id-type-owner', LServer, ["and id='", ID, "';"]) of
                [{_ID, Type, Owner}] ->
                    case list_share_users_privilige(LServer, Project, BareJID, Job, AdminJID, 0, 0, 0, ID, Type, Owner) of
                        allowed ->
                            case ejabberd_odbc:sql_query(LServer, ["select userjid from share_users where folder='", ID, "';"]) of
                                {selected, _, R} ->
                                    Users = [E || {E} <- R],
                                    F = mochijson2:encoder([{utf8, true}]),
                                    {ok, iolist_to_binary(F([Owner | Users]))};
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

list_version_ex(LServer, BareJID, ID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            case query_self_parent_info('id-uuid-size_byte-creator-created_at#id-type-owner', LServer,  ID, <<"file">>) of
                [{SelfID, SelfUUID, SelfSize, SelfCreator, SelfTime, ParentID, ParentType, ParentOwner}] ->
                    case list_version_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) of
                        allowed ->
                            case ejabberd_odbc:sql_query(LServer, ["select id, file, uuid, size_byte, creator, created_at from file_version where file='", ID, "';"]) of
                                {selected, _, R} ->
                                    R1 = [{<<"-1">>, SelfID, SelfUUID, SelfSize, SelfCreator, SelfTime} | R],
                                    ResultJson = lists:foldl(fun({E1,E2,E3,E4,E5,E6}, AccIn) ->
                                        AccIn1 = if
                                                     AccIn =:= <<>> -> <<>>;
                                                     true -> <<AccIn/binary , ",">>
                                                 end,
                                        <<AccIn1/binary, "{\"id\":\"", E1/binary, "\", \"file\":\"", E2/binary, "\", \"uuid\":\"", E3/binary,
                                        "\", \"size\":\"", E4/binary, "\", \"creator\":\"", E5/binary, "\", \"time\":\"", E6/binary, "\"}">>
                                        end,
                                    <<>>,
                                    R1),
                                    {ok, <<"[", ResultJson/binary, "]">>};
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

download_ex(LServer, BareJID, ID, UUID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            case query_self_parent_info('uuid-version_count#id-type-owner', LServer, ID, <<"file">>) of
                [{SelfUUID, Count, ParentID, ParentType, ParentOwner}] ->
                    Valid = if
                                SelfUUID =:= UUID -> true;
                                (SelfUUID /= UUID) and (Count /= <<"1">>) ->
                                    case ejabberd_odbc:sql_query(LServer, ["select uuid from file_version where file='", ID, "'"]) of
                                        {selected, _, R} ->
                                            case lists:keyfind(UUID, 1, R) of
                                                false -> false;
                                                _ -> true
                                            end;
                                        _ -> false
                                    end;
                                true -> false
                            end,
                    if
                        Valid =:= true ->
                            case download_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) of
                                allowed ->
                                    case ejabberd_odbc:sql_query(LServer, ["select uid, owner, type from mms_file where id='", UUID, "';"]) of
                                        {selected, _, [{FileUUID, _, FileType}]} ->
                                            case FileType of
                                                ?PROJECT ->
                                                    case mod_mms:get_url(FileUUID, FileType) of
                                                        error -> {error, ?AFT_ERR_LOGIC_SERVER};
                                                        URL ->
                                                            URL_Binary = iolist_to_binary(URL),
							                                Result = <<"{\"id\":\"", ID/binary, "\", \"uuid\":\"", UUID/binary,
                                                                        "\", \"url\":\"", URL_Binary/binary, "\"}">>,
							                                {ok, Result}
                                                    end;
                                                _ ->
                                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                                            end;
                                        {selected, _, []} ->
                                            {error, ?AFT_ERR_FILE_NOT_EXIST};
                                        _ ->
                                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                                    end;
                                not_allowed ->
                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                            end;
                        true ->
                            {error, ?AFT_ERR_INVALID_FILE_ID}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

get_log_ex(LServer, BareJID, Before, After, Count, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, _, _} ->
            LogList =
            if
                Before /= false ->
                    AndID = if Before =:= <<>> -> "  "; true -> [" and id<'", integer_to_binary(Before), "' "] end,
                    {selected, _, R} = ejabberd_odbc:sql_query(LServer, ["select id, userjid, operation, text, path, created_at from library_log where project='",
                        Project, "' ", AndID, " order by id desc limit ", integer_to_binary(Count), ";"]), R;
               true ->
                    {selected, _, R} = ejabberd_odbc:sql_query(LServer, ["select id, userjid, operation, text, path, created_at from library_log where project='",
                    Project, "' and id>'", integer_to_binary(After), "' order by id asc limit ", integer_to_binary(Count), ";"]), R
            end,
            LogsJson = [{struct, [{<<"id">>, R1}, {<<"jid">>, R2}, {<<"operation">>, R3},{<<"text">>, R4}, {<<"path">>, R5},
                {<<"time">>, R6}, {<<"project">>, Project}]} || {R1, R2, R3, R4, R5, R6} <- LogList],
            F = mochijson2:encoder([{utf8, true}]),
            ResultCount = length(LogList),
            {ok, iolist_to_binary(F({struct, [{<<"count">>, ResultCount}, {<<"logs">>, LogsJson}]}))};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

get_trash_ex(LServer, BareJID, Before, After, Count, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            AdminClause = if BareJID =:= AdminJID -> <<"1">>; true -> <<"0">> end,
            SelectPart = ["select file.id, file.name, file.location, file.deleted_at, folder.owner from file, folder ",
                "where file.status='0' and folder.project='", Project ,"'  and file.folder=folder.id and ( folder.owner='", escape(BareJID),
                "' or (folder.owner='admin' and '", AdminClause,"') or (folder.owner='", Job, "') ) "],
            OrderLimit = if
                             Before /= false ->
                                 AndID = if Before =:= <<>> -> "  "; true -> [" and file.deleted_at <'", integer_to_binary(Before), "' "] end,
                                [AndID, " order by file.deleted_at desc limit ", integer_to_binary(Count), ";"];
                            After /= false  ->
                                [" and file.deleted_at >'", integer_to_binary(After), "' order by file.deleted_at asc limit ", integer_to_binary(Count), ";"]
                         end,
            {selected, _, DeleteFiles} = ejabberd_odbc:sql_query(LServer, [SelectPart, OrderLimit]),
            FileJson = build_trash_file_result(DeleteFiles),
            ResultCount = integer_to_binary(length(DeleteFiles)),
            {ok, <<"{\"count\":\"", ResultCount/binary, "\", \"file\":", FileJson/binary, "}">>};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

clear_trash_ex(LServer, BareJID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            SBareJID = escape(BareJID),
            AdminClause = if BareJID =:= AdminJID -> <<"1">>; true -> <<"0">> end,
            DeleteVersionFile= ["delete file_version.* from file_version, file, folder ",
                "where folder.project='", Project, "' and file.status='0' and file.folder=folder.id and file_version.file=file.id and ( folder.owner='",
                SBareJID, "' or (folder.owner='admin' and '", AdminClause,"') or (folder.owner='", Job, "') );"],
            DeleteFile = ["delete file.* from file, folder where folder.project='", Project, "' and file.status='0' and file.folder=folder.id and ( folder.owner='",
                SBareJID, "' or (folder.owner='admin' and '", AdminClause,"') or (folder.owner='", Job, "') );"],
            DeleteFolder = ["delete from folder where project='", Project, "' and status='0' and ( owner='", SBareJID,
                "' or (folder.owner='admin' and '", AdminClause,"') or ( folder.owner='", Job, "') );"],

            F = fun() ->
                ejabberd_odbc:sql_query_t([DeleteVersionFile]),
                ejabberd_odbc:sql_query_t([DeleteFile]),
                ejabberd_odbc:sql_query_t([DeleteFolder]),
                ok
            end,
            case ejabberd_odbc:sql_transaction(LServer, F) of
                {atomic, ok} -> ok;
                _ -> {error, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

recover_file_ex(LServer, BareJID, ID, Name, DestFolder, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            case query_parent_info('id-type-name-owner-2', LServer, ID, <<"file">>) of
                [{ParentID, ParentType, ParentName, ParentOwner}] ->
                    case query_folder_info('type-owner', LServer, ["and id='", DestFolder, "';"]) of
                        [{DestType, DestOwner}] ->
                            %case recover_file_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project, DestFolder, DestType, DestOwner, Job) of
                            case recover_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, DestFolder, DestType, DestOwner) of
                                allowed ->
                                    F = fun() ->
                                        case ejabberd_odbc:sql_query_t(["select count(id) from file where folder='", DestFolder, "' and name='", escape(Name), "' and status='1';"]) of
                                            {selected,_,[{<<"0">>}]} ->
                                                {updated, 1} = ejabberd_odbc:sql_query_t(["update file set status='1', name='", escape(Name), "', folder='",
                                                    DestFolder, "' where id='", ID, "';"]),
                                                ok;
                                            _ ->
                                                name_exist
                                        end
                                    end,
                                    case ejabberd_odbc:sql_transaction(LServer, F) of
                                        {atomic, ok} ->
                                            if
                                                (DestType =:= ?TYPE_PUBLIC) ->
                                                    store_log(LServer, BareJID, ?LOG_RECOVER_FILE, Name, <<ParentName/binary, ">", Name/binary>>, Project);
                                                (DestFolder =:= ?TYPE_PUB_SUB) ->
                                                    [{PPName}] = query_parent_info(name, LServer, ParentID, <<"folder">>),
                                                    store_log(LServer, BareJID, ?LOG_RECOVER_FILE, Name, <<PPName/binary, ">", ParentName/binary, ">", Name/binary>>, Project);
                                                true ->
                                                    nothing_to_do
                                            end,
                                            ok;
                                        {atomic, name_exist} ->
                                            {error, ?AFT_ERR_FILE_EXIST}

                                    end;
                                not_allowed ->
                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                            end;
                        _ ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {is_member, finished, _, _} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

get_normal_uuid_ex(LServer, BareJID, IDList, Project) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, _, _} ->
            SBareJID = escape(BareJID),
            List = parse_json_list([<<"id">>, <<"uuid">>], IDList, []),
            {ValidList, InvalidList} = check_id_uuid_valid(LServer, Project, List, BareJID),
            {InsertValues, ReturnValidList} =
                lists:foldl(fun({E_ID, E_UUID, E_FileName}, {AccIn1, AccIn2}) ->
                    NewUUID = jlib:generate_uuid(),
                    SE_FileName = escape(E_FileName),
                    AccIn1_1 = if
                                 AccIn1 =:= <<>> -> <<>>;
                                 true -> <<AccIn1/binary, ",">>
                             end,
                    AccIn2_1 = if
                                 AccIn2 =:= <<>> -> <<>>;
                                 true -> <<AccIn2/binary, ",">>
                             end,
                    Real_UUID = case ejabberd_odbc:sql_query(LServer, ["select uid from mms_file where id='", E_UUID, "';"]) of
                                    {selected, _, [{R}]} -> R;
                                    _ -> <<>>
                                end,
                    {<<AccIn1_1/binary, "('", NewUUID/binary, "', '", Real_UUID/binary, "', '", SE_FileName/binary, "', '2', '", SBareJID/binary,"')">>,
                        <<AccIn2_1/binary, "{\"id\":\"", E_ID/binary, "\", \"uuid\":\"", E_UUID/binary, "\", \"normal_uuid\":\"", NewUUID/binary, "\"}">>}
                end,
                {<<>>, <<>>},
                ValidList),

            if
                (ValidList =:= []) and (InvalidList =:= []) ->
                    {ok, <<"[]">>};
                true ->
                    InsertResult = case InsertValues of
                                       <<>> -> ok;
                                       _ ->
                                           Query = ["insert into mms_file (id, uid, filename, type, owner) values ", InsertValues],
                                           case ejabberd_odbc:sql_query(LServer, Query) of
                                               {updated, _} -> ok;
                                               _ ->
                                                   error
                                           end
                                   end,
                    case InsertResult of
                        ok ->
                            ReturnList = lists:foldl(fun(E, AccIn) ->
                                case E of
                                    {E_ID, E_UUID} ->
                                        case AccIn of
                                            <<>> -> <<"{\"id\":\"", E_ID/binary, "\", \"uuid\":\"", E_UUID/binary, "\", \"normal_uuid\":\"\"}">>;
                                            _ -> <<AccIn/binary, ", {\"id\":\"", E_ID/binary, "\", \"uuid\":\"", E_UUID/binary, "\", \"normal_uuid\":\"\"}">>
                                        end;
                                    {E_ID} ->
                                        case AccIn of
                                            <<>> -> <<"{\"id\":\"", E_ID/binary, "\", \"uuid\":\"\", \"normal_uuid\":\"\"}">>;
                                            _ -> <<AccIn/binary, ", {\"id\":\"", E_ID/binary, "\", \"uuid\":\"\", \"normal_uuid\":\"\"}">>
                                        end
                                end
                            end,
                                ReturnValidList,
                                InvalidList),
                            {ok, <<"[", ReturnList/binary, "]">>};
                        error ->
                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                    end
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.
check_id_uuid_valid(_LServer, _Project, [], _BareJID) ->
    {[], []};
check_id_uuid_valid(LServer, Project, List, _BareJID) ->
    %% file exist and not delete; file in project; id and uuid are right.
    {Query1Condition, Query2Condition} =
    lists:foldl(fun(E, {AccIn1, AccIn2}) ->
        case E of
            {ID} ->
                AccOut1 = case AccIn1 of
                              [] -> [" (file.id='", ID, "') "];
                              _ -> [AccIn1 | [" or ( file.id='", ID, "') "]]
                          end,
                {AccOut1, AccIn2};
            {ID, <<>>} ->
                AccOut1 = case AccIn1 of
                              [] -> [" (file.id='", ID, "') "];
                              _ -> [AccIn1 | [" or ( file.id='", ID, "') "]]
                          end,
                {AccOut1, AccIn2};
            {ID, UUID} ->
                AccOut1 = case AccIn1 of
                              [] -> [" (file.id='", ID, "' and file.uuid='", UUID, "') "];
                              _ -> [AccIn1 | [" or ( file.id='", ID, "' and file.uuid='", UUID, "') "]]
                          end,
                AccOut2 = case AccIn2 of
                              [] -> [" (fv.file='", ID, "' and fv.uuid='", UUID, "') "];
                              _ -> [AccIn2 | [" or ( fv.file='", ID, "' and fv.uuid='", UUID, "') "]]
                          end,
                {AccOut1, AccOut2}
        end
        end,
        {[],[]},
        List),
    Query1 = ["select file.id, file.uuid, file.name from file, folder where (file.folder=folder.id and folder.project='",
        Project, "' and file.status='1') and (", Query1Condition, ")"],
    Query2 = ["select fv.file, fv.uuid, f.name from file_version as fv, file as f, folder as fo where (fv.file=f.id and f.folder=fo.id and fo.project='",
        Project, "' and f.status='1') and ( ", Query2Condition, " )"],
    F = fun() ->
        {selected, _, R1} = ejabberd_odbc:sql_query_t(Query1),
        R2 = case Query2Condition of
                 [] ->
                     [];
                 _ ->
                     {selected, _, Result} = ejabberd_odbc:sql_query_t(Query2),
                     Result
             end,
        {ok, R1, R2}
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {ok, R1, R2}} ->
            R = lists:flatten( [R1 | R2] ),
            InvalidList = lists:foldl(fun(Ele, AccIn) ->
                case Ele of
                    {Ele_ID, _Ele_UUID} ->
                        case lists:keyfind(Ele_ID, 1, R) of
                            false -> [ Ele | AccIn];
                            _ -> AccIn
                        end;
                    {Ele_ID} ->
                        case lists:keyfind(Ele_ID, 1, R) of
                            false -> [ {Ele_ID, <<>>} | AccIn];
                            _ -> AccIn
                        end
                end
                end,
                [],
                List),
            {R, InvalidList};
        _ ->
            {error, ?ERR_INTERNAL_SERVER_ERROR}
    end.

search_file_ex(LServer, BareJID, Project, Folder, Name, Page, Count) ->
    case check_status(LServer, Project, BareJID) of
        {is_member, _, Job, AdminJID} ->
            SBareJID = escape(BareJID),
            SName = escape(Name),
            StartLine = integer_to_binary((binary_to_integer(Page) - 1) * binary_to_integer(Count)),
            DepartmentLeaderOrMember =
                case query_folder_info('type-owner', LServer, ["and project='", Project, "' and id='", Folder, "';"]) of
                    [{FolderType, _TypeOwner}] ->
                    if
                        (FolderType =:= ?TYPE_DEPARTMENT) or (FolderType =:= ?TYPE_DEP_SUB) ->
                            case is_department_folder_member_or_leader(LServer, Project, Folder, Job, BareJID =:= AdminJID) of
                                true -> <<"1">>;
                                false -> <<"0">>
                            end;
                        true ->
                            <<"0">>
                    end
                end,
            Query = ["select f.id,f.uuid,f.name,f.size_byte,f.creator,f.version_count,f.folder,f.created_at from file as f, folder as fo ",
                "where (f.status='1' and fo.project='", Project, "' and (fo.owner='admin' or fo.owner='", SBareJID,
                "' or '", DepartmentLeaderOrMember, "')) and ((f.folder=fo.id and f.folder='", Folder, "') or (f.folder=fo.id and fo.parent='",
                Folder, "')) and f.name like '%", SName, "%' order by f.created_at desc limit ", StartLine, ",", Count, ";"],
            case ejabberd_odbc:sql_query(LServer, Query) of
                {selected, _, R} ->
                    FileJson = build_file_result(R),
                    {ok, <<"{\"folder\":\"", Folder/binary, "\", \"name\":\"", Name/binary, "\", \"page\":\"", Page/binary,
                    "\", \"count\":\"", Count/binary, "\", \"file\":", FileJson/binary, "}">>};
                _ ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

check_status(LServer, Project, JID) ->
    Query = ["select p.status, ou.organization, p.admin from organization_user as ou, organization as o, project as p ",
        " where ou.organization=o.id and ou.jid='", escape(JID),  "' and o.project=p.id and p.id='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{<<"0">>, JobID, AdminJID}]} ->
            {is_member, finished, JobID, AdminJID};
        {selected, _, [{<<"1">>, JobID, AdminJID}]} ->
            {is_member, running, JobID, AdminJID};
        _ ->
            not_member
    end.


%% ------------------------------------------------------------------
%% privilige function.
%% ------------------------------------------------------------------
%% TOFIX: build a mnesia ram privilige talbe may be can make code is more clear and maintenance is more effective???

recover_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, DestFolder, DestType, DestOwner) ->
    IsSrcDepartmentLeader =  if
                                 (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
                                     is_department_folder_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID);
                                 true ->
                                     false
                            end,
    IsDestDepartmentMemberOrLeader = if
                                         (DestType =:= ?TYPE_DEPARTMENT) or (DestType =:= ?TYPE_DEP_SUB) ->
                                             is_department_folder_member_or_leader(LServer, Project, DestFolder, Job, BareJID =:= AdminJID);
                                         true ->
                                             false
                                     end,

    if
        ((ParentOwner =:= <<"admin">>) or (ParentOwner =:= BareJID)) or (((ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB)) and IsSrcDepartmentLeader) ->
            if
                (DestOwner =:= <<"admin">>) and ((DestType =:= ?TYPE_PUBLIC) or ((DestType =:= ?TYPE_PUB_SUB))) ->
                    allowed;
                (DestOwner =:= BareJID) ->
                    allowed;
                ((DestType =:= ?TYPE_DEPARTMENT) or (DestType =:= ?TYPE_DEP_SUB)) and IsDestDepartmentMemberOrLeader->
                    allowed;
                true ->
                    not_allowed
            end;
        true ->
            not_allowed
    end.

list_version_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB)->
            allowed;
        (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_PRIVATE)->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        (ParentType =:= ?TYPE_PER_SHARE) ->
            case is_user_shared_in_folder(LServer, ParentID, BareJID) of
                true ->
                    allowed;
                _ ->
                    if
                        BareJID =:= ParentOwner -> allowed;
                        true -> not_allowed
                    end
            end;
        (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
            case is_department_folder_member_or_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                false -> not_allowed
            end
    end.

list_share_users_privilige(LServer, _Project, BareJID, _Job, _AdminJID, _ParentID, _ParentType, _ParentOwner, SelfID, SelfType, SelfOwner) ->
    if
        (SelfType =:= ?TYPE_PER_SHARE) ->
            %% ------------------- shoud check???
            case is_user_shared_in_folder(LServer, SelfID, BareJID) of
                true ->
                    allowed;
                _ ->
                    if
                        BareJID =:= SelfOwner -> allowed;
                        true -> not_allowed
                    end
            end;
            %% -------------------
       true -> not_allowed
    end.

download_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) ->
            allowed;
        (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        ParentType =:= ?TYPE_PER_SHARE ->
            case is_user_shared_in_folder(LServer, ParentID, BareJID) of
                true ->
                    allowed;
                _ ->
                    if
                        BareJID =:= ParentOwner -> allowed;
                        true -> not_allowed
                    end
            end;
        (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
            case is_department_folder_member_or_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                _ -> not_allowed
            end;
        true ->
            not_allowed
    end.

add_version_privilige(LServer, Project, BareJID, Job, AdminJID,  ParentID, ParentType, ParentOwner) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) ->
            allowed;
        (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
            if
                (BareJID =:= ParentOwner) -> allowed;
                true -> not_allowed
            end;
        (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
            case is_department_folder_member_or_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                _ -> not_allowed
            end;
        true -> not_allowed
    end.

% folder: personal sub folder ---> public folder or his department folder
%         department sub folder ---> public folder
%
% file: personal file ---> personal other folder or public [sub-]folder  or his department [sub-]folder
%       department file ---> department other folder or public [sub-]folder
move_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, SelfType2, _SelfUpdater, DestFolder, DestType, DestOwner) ->
    if
        (ParentID =:= DestFolder) -> not_allowed;
        true ->
            IsSrcDepartmentLeader = if
                                        (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
                                            is_department_folder_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID);
                                        true ->
                                            false
                                    end,
            IsDestDepartmentMemberOrLeader = if
                                                 (DestType =:= ?TYPE_DEPARTMENT) or (DestType =:= ?TYPE_DEP_SUB) ->
                                                     is_department_folder_member_or_leader(LServer, Project, DestFolder, Job, BareJID =:= AdminJID);
                                                 true ->
                                                     false
                                             end,
            if
                (SelfType2 =:= <<"folder">>) ->
                    if
                        (ParentType =:= ?TYPE_PERSON) and (BareJID =:= ParentOwner) ->
                            if
                                (DestType =:= ?TYPE_PUBLIC) -> allowed;
                                (DestType =:= ?TYPE_DEPARTMENT) and IsDestDepartmentMemberOrLeader-> allowed;
                                true -> not_allowed
                            end;
                        (ParentType =:= ?TYPE_DEPARTMENT) and IsSrcDepartmentLeader and (DestType =:= ?TYPE_PUBLIC) -> allowed;
                        true -> not_allowed
                    end;
                true ->
                    if
                        ((ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE)) and (BareJID =:= ParentOwner) ->
                            if
                                ((DestType =:= ?TYPE_PERSON) or (DestType =:= ?TYPE_PER_SHARE) or (DestType =:= ?TYPE_PER_PRIVATE)) and (BareJID =:= DestOwner) -> allowed;
                                ((DestType =:= ?TYPE_DEPARTMENT) or (DestType =:= ?TYPE_DEP_SUB)) and IsDestDepartmentMemberOrLeader -> allowed;
                                ((DestType =:= ?TYPE_PUBLIC) or (DestType =:= ?TYPE_PUB_SUB)) -> allowed;
                                true -> not_allowed
                            end;
                        ((ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB)) and IsSrcDepartmentLeader ->
                            if
                                ((DestType =:= ?TYPE_DEPARTMENT) or (DestType =:= ?TYPE_DEP_SUB)) and (IsDestDepartmentMemberOrLeader) -> allowed;
                                (DestType =:= ?TYPE_PUBLIC) or (DestType =:= ?TYPE_PUB_SUB) -> allowed;
                                true -> not_allowed
                            end;
                        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) ->
                            if
                                ((DestType =:= ?TYPE_PUBLIC) or (DestType =:= ?TYPE_PUB_SUB)) and (BareJID =:= AdminJID) ->
                                    allowed;
                                true ->
                                    not_allowed
                            end;
                        true -> not_allowed
                    end
            end
    end.

share_privilige(_LServer, _Project, BareJID, _Job, _AdminJID, _ParentID, ParentType, ParentOwner, SelfType) ->
    if
        (ParentType =:= ?TYPE_PERSON) and ( (SelfType =:= ?TYPE_PER_SHARE) or (SelfType =:= ?TYPE_PER_PRIVATE) ) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        true ->
            not_allowed
    end.

rename_folder_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, SelfUpdater) ->
    case ParentType of
        ?TYPE_PUBLIC ->
            if
                BareJID =:= AdminJID -> allowed;
                true ->
                    if
                        BareJID =:= SelfUpdater -> allowed;
                        true -> not_allowed
                    end
            end;
        ?TYPE_PERSON ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        ?TYPE_DEPARTMENT ->
            case is_department_folder_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                false ->
                    if
                        BareJID =:= SelfUpdater -> allowed;
                        true -> not_allowed
                    end
            end;
        _ ->
            not_allowed
    end.
rename_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner, SelfUpdater) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) ->
            if
                BareJID =:= AdminJID -> allowed;
                true ->
                    if
                        BareJID =:= SelfUpdater -> allowed;
                        true -> not_allowed
                    end
            end;
        (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
            case is_department_folder_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                false ->
                    if
                        BareJID =:= SelfUpdater -> allowed;
                        true -> not_allowed
                    end
            end;
        true ->
            not_allowed
    end.

delete_folder_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) ->
    case ParentType of
        ?TYPE_PUBLIC ->
            if
                BareJID =:= AdminJID -> allowed;
                true -> not_allowed
            end;
        ?TYPE_PERSON ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        ?TYPE_DEPARTMENT ->
            case is_department_folder_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                _ -> not_allowed
            end;
        _ ->
            not_allowed
    end.

delete_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) ->
            if
                BareJID =:= AdminJID -> allowed;
                true -> not_allowed
            end;
        (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        (ParentType =:= ?TYPE_DEPARTMENT) or (ParentType =:= ?TYPE_DEP_SUB) ->
            case is_department_folder_leader(LServer, Project, ParentID, Job,  BareJID =:= AdminJID) of
                true -> allowed;
                _ -> not_allowed
            end;
        true ->
            not_allowed
    end.

create_folder_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) ->
    case ParentType of
        ?TYPE_PUBLIC ->
            {allowed, ?TYPE_PUB_SUB};
        ?TYPE_PERSON ->
            if
                BareJID =:= ParentOwner ->
                    {allowed, ?TYPE_PER_PRIVATE};
                true ->
                    not_allowed
            end;
        ?TYPE_DEPARTMENT ->
            case is_department_folder_member_or_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> {allowed, ?TYPE_DEP_SUB};
                false -> not_allowed
            end;
        _ ->
            not_allowed
    end.

create_file_privilige(LServer, Project, BareJID, Job, AdminJID, ParentID, ParentType, ParentOwner) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) ->
            allowed;
        (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_PRIVATE)->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        (ParentType =:= ?TYPE_PER_SHARE) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true ->
                    case is_user_shared_in_folder(LServer, ParentID, BareJID) of
                        true -> allowed;
                        false -> not_allowed
                    end
            end;
        (ParentType =:= ?TYPE_DEPARTMENT) or(ParentType =:= ?TYPE_DEP_SUB)->
            case is_department_folder_member_or_leader(LServer, Project, ParentID, Job, BareJID =:= AdminJID) of
                true -> allowed;
                _ -> not_allowed
            end;
        true ->
            not_allowed
    end.


%% ------------------------------------------------------------------
%% other help function.
%% ------------------------------------------------------------------

%% check job.department = folder.owner.department

%% build folder query sql.
folder_sql_query(LServer, Type, BareJID, JobID, Folder, Project, IsOwner, IsAdmin) ->
    case Type of
        ?TYPE_ROOT ->
            Q1 = [query_folder_column(normal), "and project='", Project, "' and (type='", ?TYPE_PUBLIC, "' or (type='", ?TYPE_PERSON, "' and owner='", escape(BareJID),"'));"],
            Q2 = ["select f.id, f.type, f.name, f.creator, f.owner, f.created_at, f.department_id from folder as f, organization as o where f.project='", Project,"' and o.project='",
                Project, "' and f.type='", ?TYPE_DEPARTMENT, "' and f.status='1' and f.department_id=o.department_id and o.id='", JobID, "';"],
            [{folder, Q1}, {folder, Q2}];
        ?TYPE_SHARE ->
            Query = ["select id, type, name, creator, owner, created_at, department_id from folder, share_users where folder.status='1' and folder.type='",
                ?TYPE_PER_SHARE, "' and folder.owner <> '", escape(BareJID),"' and folder.id=share_users.folder and share_users.userjid='",
                escape(BareJID), "' and folder.project='", Project, "';"],
            [{folder, Query}];
        ?TYPE_ADMIN_MANAGER ->
            if
                IsAdmin =:= true ->
                    Query = ["select id, type, name, creator, owner, created_at, department_id from folder where project='", Project,
                        "' and type='", ?TYPE_DEPARTMENT, "' and status='1' and folder.owner='admin';" ],
                        [{folder, Query}];
                true ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        ?TYPE_PUBLIC ->
            Query = [query_folder_column(normal), "and parent='", Folder, "';"],
            [{file, query_file_by_parent(Folder)}, {folder, Query}];
        ?TYPE_PUB_SUB ->
            [{file, query_file_by_parent(Folder)}];
        ?TYPE_PERSON ->
            if
               IsOwner =:= true ->
                   Query = [query_folder_column(normal), "and parent='", Folder, "' and owner='", escape(BareJID), "';"],
                   [{file, query_file_by_parent(Folder)}, {folder, Query}];
               true ->
                   {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        ?TYPE_PER_SHARE ->
            if
                IsOwner =:= true -> [{file, query_file_by_parent(Folder)}];
                true ->
                    case ejabberd_odbc:sql_query(LServer, ["select folder from share_users where folder='", Folder,
                        "' and userjid='", escape(BareJID), "';"]) of
                        {selected, _, [{_ID}]} -> [{file, query_file_by_parent(Folder)}];
                        _ -> {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end
            end;
        ?TYPE_PER_PRIVATE ->
            if
                IsOwner =:= true -> [{file, query_file_by_parent(Folder)}];
                true -> {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        ?TYPE_DEPARTMENT ->
                case is_department_folder_member_or_leader(LServer, Project, Folder, JobID, IsAdmin) of
                    true ->
                        Query = [query_folder_column(normal), "and parent='", Folder, "';"],
                        [{file, query_file_by_parent(Folder)}, {folder, Query}];
                    _ ->
                        {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                end;
        ?TYPE_DEP_SUB ->
            case is_department_folder_member_or_leader(LServer, Project, Folder, JobID, IsAdmin) of
                true ->
                    [{file, query_file_by_parent(Folder)}];
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        _ ->
            []
    end.

query_file_by_parent(Folder) ->
    ["select id, uuid, name, size_byte, creator, version_count, folder, created_at from file where folder='", Folder, "' and status='1';"].


build_folder_result(List) ->
    Result =
        lists:foldl(fun({ID, Type, Name, Creator, Owner, Time, PartID}, AccIn) ->
            AccIn1 = case AccIn of
                         <<>> -> <<>>;
                         _ -> <<AccIn/binary, ",">>
                     end,
            <<AccIn1/binary, "{\"id\":\"", ID/binary, "\", \"type\":\"", Type/binary, "\", \"name\":\"", Name/binary,"\", \"creator\":\"",
            Creator/binary, "\", \"owner\":\"", Owner/binary, "\", \"time\":\"", Time/binary, "\", \"part_id\":\"", PartID/binary, "\"}">>
            end,
            <<>>,
            List),
    <<"[", Result/binary, "]">>.

build_trash_file_result(List) ->
    Result =
        lists:foldl(fun({E1, E2, E3, E4, E5}, AccIn) ->
            AccIn1 = case AccIn of
                         <<>> -> <<>>;
                         _ -> <<AccIn/binary, ",">>
                     end,
            <<AccIn1/binary, "{\"id\":\"", E1/binary, "\", \"name\":\"", E2/binary, "\", \"location\":\"", E3/binary,
            "\", \"delete_time\":\"", E4/binary, "\", \"owner\":\"", E5/binary, "\"}">>
        end,
            <<>>,
            List),
    <<"[", Result/binary, "]">>.

build_file_result(List) ->
    Result =
        lists:foldl(fun({ID, UUID, Name, Size, Creator, VersionCount, Folder, Time}, AccIn) ->
                AccIn1 = case AccIn of
                             <<>> -> <<>>;
                             _ -> <<AccIn/binary, ",">>
                         end,
                <<AccIn1/binary, "{\"id\":\"", ID/binary, "\", \"uuid\":\"", UUID/binary, "\", \"name\":\"", Name/binary,
                "\", \"size\":\"", Size/binary,  "\", \"creator\":\"", Creator/binary, "\", \"version_count\":\"",VersionCount/binary,
                "\", \"folder\":\"", Folder/binary, "\", \"time\":\"", Time/binary, "\"}">>
            end,
            <<>>,
            List),
    <<"[", Result/binary, "]">>.


query_self_parent_info(Column, LServer, ID, Type) ->
    Query = case Type of
                <<"folder">> -> [query_self_parent_column(Column, Type), "and f1.id='", ID, "' and f2.id=f1.parent;"];
                <<"file">> -> [query_self_parent_column(Column, Type), "and f1.id='", ID, "' and f2.id=f1.folder;"]
            end,
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            not_exist;
        {selected, _, R} ->
            R;
        _ ->
            error
    end.

filter_query_ending(Front, Ending) ->
    LastString = lists:flatten(lists:last(Front)),
    Ending1 = case string:right(LastString, 2) of
                  "' " -> Ending;
                  "  " ->
                      FirstString = string:strip(lists:nth(1, Ending), left),
                      NewFirString = case string:left(FirstString, 3) of
                                         "and" -> string:sub_string(FirstString, 4);
                                         _ -> FirstString
                                     end,
                      Tail = lists:sublist(Ending, 2, length(Ending) - 1),
                      [NewFirString | Tail]
              end,
    [Front | Ending1].

query_parent_info(Column, LServer, ID, Type) ->
    Query = case Type of
                <<"folder">> -> filter_query_ending(query_folder_column(Column), [" and id=(select parent from folder where id='", ID, "');"]);
                <<"file">> -> filter_query_ending(query_folder_column(Column), [" and id=(select folder from file where id='", ID, "');"])
            end,
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            not_exist;
        {selected, _, R} ->
            R;
        _ ->
            error
    end.

query_folder_info(Column, LServer, QueryEnding) ->
    Query = filter_query_ending(query_folder_column(Column), QueryEnding),
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, []} ->
            not_exist;
        {selected, _, R} ->
            R;
        _ ->
            error
    end.

parse_column(Data) ->
    StringData = case is_atom(Data) of
                     true -> atom_to_list(Data);
                     _ -> Data
                 end,
    {Length, Value} = case string:right(StringData, 2) of
                          "-0" -> {2, 0};
                          "-1" -> {2, 1};
                          "-2" -> {2, 2};
                          _ -> {0, 1}
                      end,
    {string:left(StringData, string:len(StringData) - Length), integer_to_binary(Value)}.


query_folder_column(Column) ->
    {StrColumn1, Status} = parse_column(Column),
    StrColumn2 = case  Column of
                    normal ->
                        "id, type, name, creator, owner, created_at, department_id";
                    _ ->
                        lists:map(fun(E) -> if E =:= $- -> $,; true -> E end end, StrColumn1)
                end,
    WhereStatus = if
                      Status =:= <<"2">> ->
                          "  "; %% two space.
                      true ->
                          [" status='", Status, "' "] %% ' + space.
                  end,
    ["select ", StrColumn2, " from folder where", WhereStatus].


query_file_column(Column) ->
    case Column of
        normal ->
            "select id, uuid, name, size_byte, creator, version_count, folder, created_at from file where status='1' ";
        owner ->
            "select owner from file where status='1' "
    end.


query_self_parent_column(Column, Type) ->
    [SelfColumn, ParentColumn] = string:tokens(atom_to_list(Column), "#"),
    {SColumn1, SStatus1} = parse_column(SelfColumn),
    {PColumn1, PStatus1} = parse_column(ParentColumn),
    SColumn2 = lists:flatten([ "f1.", lists:map(fun(E) -> if E =:= $- -> ",f1."; true -> E end end, SColumn1)]),
    PColumn2 = lists:flatten([ "f2.", lists:map(fun(E) -> if E =:= $- -> ",f2."; true -> E end end, PColumn1)]),

    SelfStatus = if
                     SStatus1 =:= <<"2">> ->
                         " ";
                     true ->
                         [" f1.status='", SStatus1, "' "]
                 end,
    ParentStatus = if
                       PStatus1 =:= <<"2">> ->
                           " ";
                       true ->
                           if
                               SelfStatus /= " " ->
                                   [" and f2.status='", PStatus1, "' "];
                               true ->
                                   [" f2.status='", PStatus1, "' "]
                           end
                   end,

    ["select ", SColumn2, ",", PColumn2, " from ", Type, " as f1, folder as f2 where", SelfStatus, ParentStatus].

get_file_dir(LServer, ParentID, ParentType, ParentName) ->
    get_dir(LServer, <<"file">>, ParentID, ParentType, ParentName).

get_folder_dir(LServer, ParentID, ParentType, ParentName) ->
    get_dir(LServer, <<"folder">>,  ParentID, ParentType, ParentName).

get_dir(LServer, Type, ParentID, ParentType, ParentName) ->
    if
        Type =:= <<"file">> ->
            if
                (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_DEPARTMENT) ->
                    {ok, ParentName};
                (ParentType =:= ?TYPE_PERSON) ->
                    {ok, <<"@">>};
                (ParentType =:= ?TYPE_PUB_SUB) or (ParentType =:= ?TYPE_PER_SHARE)
                    or (ParentType =:= ?TYPE_PER_PRIVATE) or (ParentType =:= ?TYPE_DEP_SUB) ->
                    case ejabberd_odbc:sql_query(LServer, ["select f2.name from folder as f1, folder as f2 where f1.id='",
                        ParentID, "' and f1.parent=f2.id;"]) of
                        {selected, _, [{PPName}]} ->
                            if
                                (ParentType =:= ?TYPE_PUB_SUB) or (ParentType =:= ?TYPE_DEP_SUB)->
                                    {ok, <<PPName/binary, ">", ParentName/binary>>};
                                true ->
                                    {ok, <<"@>", ParentName/binary>>}
                            end;
                        _ -> not_exist
                    end
            end;
       Type =:= <<"folder">> ->
           if
               (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_DEPARTMENT) ->
                   {ok, ParentName};
               (ParentType =:= ?TYPE_PERSON) ->
                   {ok, <<"@">>}
           end
    end.

binary_to_integer(Bin, OpenFiler, MinValue, MaxValue) ->
    Result = case catch binary_to_integer(Bin) of
                 {'EXIT', _} -> error;
                 Value -> Value
             end,
    if
        OpenFiler ->
            case Result of
                error ->

                    MaxValue;
                V ->
                    if
                        ( V > MaxValue) -> MaxValue;
                        (V < MinValue) -> MinValue;
                        true -> V
                    end
            end;
        true ->
            Result
    end.

check_log_trash_parameter(Before, After, Count) ->
    if (Before /= false) and (After /= false) -> error;
        (Before =:= false) and (After =:= false) -> error;
        true ->
            CountInteger = binary_to_integer(Count, true, 1, 20),
            if
                (Before /= false)->
                    if
                        Before =:= <<>> ->
                            {<<>>, false, CountInteger};
                        true ->
                            BeforeID = binary_to_integer(Before, false, 0, 0),
                            if
                                (BeforeID =:= error) ->error;
                                true -> {BeforeID, false, CountInteger}
                            end
                    end;
                (After /= false) ->
                    if
                        After =:= <<>> ->
                            {false, <<>>, CountInteger};
                        true ->
                            AfterID = binary_to_integer(After, false, 0, 0),
                            if
                                (AfterID =:= error) ->error;
                                true -> {false, AfterID, CountInteger}
                            end
                    end
            end
    end.

check_add_file_uuid_valid(LServer, JID, UUID, FileID) ->
    case ejabberd_odbc:sql_query(LServer, ["select id, type from mms_file where id='", UUID, "'"]) of
        {selected,_,[{ID, <<"3">>}]} ->
            case FileID of
                false ->
                    {valid, <<"3">>, <<"">>};
                _ ->
                    %% check from_project folder privilege ??? this only check JID is member of from_roject.
                    case ejabberd_odbc:sql_query(LServer, ["select folder.project from file, folder, file_version where (folder.id=file.folder) ",
                        " and (file.id='", FileID, "') and (file.uuid='", UUID,
                        "' or (file.version_count >'1' and file_version.file = file.id and file_version.uuid='", UUID, "')) limit 1;"]) of
                        {selected, _, [{FromProject}]} ->
                            case odbc_organization:is_member(LServer, FromProject, JID) of
                                true ->
                                    {valid, <<"3">>, UUID};
                                _ ->
                                    invalid
                            end;
                        _ ->
                            invalid
                    end
            end;
        {selected,_, [{ID, <<"2">>}]} ->
            LibUUID = jlib:generate_uuid(),
            {updated, 1} = ejabberd_odbc:sql_query(LServer, ["insert into mms_file(id, filename, owner, uid, type) ",
                " select '", LibUUID, "', filename, '", escape(JID),"', uid, '3' from mms_file where id='", ID,"';"]),
            {valid, <<"2">>, LibUUID};
        _ ->
            invalid
    end.

store_log(LServer, JID, Operation, Text, Path, Project) ->
    Pid = srv_name(LServer),
    Pid ! {JID, Operation, Text, Path, Project}.

escape(Name) ->
    ejabberd_odbc:escape(Name).

now_to_microseconds({Mega, Secs, Micro}) ->
    (1000000 * Mega + Secs) * 1000000 + Micro.

microseconds_to_now(MicroSeconds) when is_integer(MicroSeconds) ->
    Seconds = MicroSeconds div 1000000,
    {Seconds div 1000000, Seconds rem 1000000, MicroSeconds rem 1000000}.

parse_item([], _, Result) ->
    list_to_tuple(lists:reverse(Result));
parse_item([H | T], List, Result) ->
    R = case lists:keyfind(H, 1, List) of
            false -> Result;
            {H, Value} -> [Value | Result]
        end,
    parse_item(T, List, R).
parse_json_list(_, [], Result) ->
    lists:reverse(Result);
parse_json_list(Keys, [{struct, H} | T], Result) ->
    R = parse_item(Keys, H, []),
    parse_json_list(Keys, T, [R | Result]).


%% {_, ID} = lists:keyfind(<<"id">>, 1, Data),
%% Users = case lists:keyfind(<<"users">>, 1, Data) of
%%     {_, R} -> R;
%%     _ -> false
%%     end,

find_key_value(List, Key) ->
    find_key_value(List, Key, false).
find_key_value(List, Key, DefaultValue) ->
    find_key_value(List, Key, 1, DefaultValue).
find_key_value(List, Key, KeyPosition, DefaultValue) ->
    KeyPos = if
                 is_binary(KeyPosition) -> binary_to_integer(KeyPosition);
                 is_integer(KeyPosition) -> KeyPosition;
                 true -> 1
             end,
    case lists:keyfind(Key, KeyPos, List) of
        {_, R} -> R;
        _ -> DefaultValue
    end.

is_user_shared_in_folder(LServer, Folder, BareJID) ->
    case ejabberd_odbc:sql_query(LServer, ["select count(folder) from share_users where folder='",
        Folder, "' and userjid='", escape(BareJID), "';"]) of
        {selected, _, [{<<"1">>}]} -> true;
        _ -> false
    end.

%% check Job is Folder's owner.(folder's type must be department or dep_sub)
is_department_folder_leader(LServer, Project, Folder, Job, IsAdmin) ->
    AdminClause = if IsAdmin =:= true -> <<"1">>; true -> <<"0">> end,
    case ejabberd_odbc:sql_query(LServer, ["select count(id) from folder where id='", Folder, "' and project='", Project,
        "' and (owner='", Job, "' or (owner='admin' and '", AdminClause, "'));"]) of
        {selected, _, [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

is_department_folder_member_or_leader(LServer, Project, Folder, Job, IsAdmin) ->
    AdminClause = if IsAdmin =:= true -> <<"1">>; true -> <<"0">> end,
    case ejabberd_odbc:sql_query(LServer, ["select count(f.id) from folder as f, organization as o where f.id='", Folder,
        "' and f.project='", Project, "' and  o.id='", Job, "' and o.project='", Project, "' and ((o.department_id=f.department_id) "
        " or (f.owner='admin' and '", AdminClause, "'));"]) of
        {selected, _, [{<<"1">>}]} ->
            true;
        _ ->
            false
    end.

%% check Job is DepartmentID's leader.
is_department_leader_Job(LServer, Project, DepartmentID, Job) ->
    case ejabberd_odbc:sql_query(LServer, ["select id from organization where project='", Project, "' and department_id='",
        DepartmentID, "' order by lft limit 1"] ) of
        {selected, _, [{Job}]} -> true;
        _ -> false
    end.

%% ------------------------------------------------------------------
%% write log process.
%% ------------------------------------------------------------------

start_worker(Host) ->
    Proc = srv_name(Host),
    ChildSpec =
        {Proc,
            {?MODULE, start_link, [Proc, Host]},
            permanent,
            5000,
            worker,
            [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec),
    ok.

stop_worker(Host) ->
    Proc = srv_name(Host),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).

start_link(Name, Host) ->
    gen_server:start_link({local, Name}, ?MODULE, [Host], []).

srv_name() ->
    mod_project_library_log.

srv_name(Host) ->
    gen_mod:get_module_proc(Host, srv_name()).

init([Host]) ->
    {ok, Host}.

handle_call(_, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({JID, Operation, Text, Path, Project} = Log, State) ->
    Insert = ["insert into library_log(userjid, operation, text, path, project) values('", escape(JID), "', '",
        Operation, "', '", escape(Text), "', '", escape(Path), "', '", Project, "');"],
    case ejabberd_odbc:sql_query(State, Insert) of
        {updated, 1} ->
            ok;
        _ ->
            ?WARNING_MSG("[ERROR]:store log error ~p.", [Log])
    end,
    {noreply, State};
handle_info(Log, State) ->
    ?WARNING_MSG("[ERROR]:Strange log ~p.", [Log]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
