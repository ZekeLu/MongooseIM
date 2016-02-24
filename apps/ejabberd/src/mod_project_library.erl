-module(mod_project_library).

-behaviour(gen_mod).

%% iq, hooks exports
-export([start/2, stop/1, process_iq/3,
    create_project/2, delete_member/3]).


%% Internal exports
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/2, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-include("jlib.hrl").
-include("ejabberd.hrl").
-include("mod_mms.hrl").

-define(TYPE_ROOT,          <<"-1">>).
-define(TYPE_PUBLIC,        <<"0">>).  % public folder.
-define(TYPE_PUB_SUB,       <<"1">>).  % public sub folder.
-define(TYPE_PERSON,        <<"2">>).  % personal folder.
-define(TYPE_PER_PUBLIC,    <<"3">>).  % personal sub public folder.
-define(TYPE_PER_SHARE,     <<"4">>).  % personal sub share folder.
-define(TYPE_PER_PRIVATE,   <<"5">>).  % personal private folder.

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
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host,
        ?NS_AFT_LIBRARY, ?MODULE, process_iq, no_queue),
    ejabberd_hooks:add(create_project, Host,
        ?MODULE, create_project, 50),
    ejabberd_hooks:add(delete_member, Host,
        ?MODULE, delete_member, 50),
    ok.

stop(Host) ->
    ejabberd_hooks:delete(delete_member, Host,
        ?MODULE, delete_member, 50),
    ejabberd_hooks:delete(create_project, Host,
        ?MODULE, create_project, 50),
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

create_project(LSever, Project) ->
    case ejabberd_odbc:sql_query(LSever, ["insert into folder(type, name, creator, owner, parent, project, status)",
        " select type, name, creator, owner, parent, ", Project, ", status from folder where project='-1';"]) of
        {updated, 3} -> %% three ?TYPE_PUBLIC folder in project.
            ok;
        _ ->
            ?ERROR_MSG("[ERROR]:Create Public folder failed for project ~p", [Project])
    end.

delete_member(LServer, BareJID, Project) ->
    clear_trash_ex(LServer, BareJID, Project).


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
                     add_file_ex(S, BareJID, Parent, Name, UUID, Size, Project)
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
    Users = case lists:keyfind(<<"users">>, 1, Data) of
                {_, R} -> R;
                _ -> false
            end,

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
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
            Querys = if
                         (Folder =:= <<>>) or (Folder =:= <<"-1">>) ->
                             folder_sql_query(LServer, ?TYPE_ROOT, BareJID, Folder, Project, false);
                         true ->
                             case query_folder_info('type-owner', LServer, ["and id='", Folder, "';"]) of
                                 [{Type, Owner}] ->
                                     folder_sql_query(LServer, Type, BareJID, Folder, Project, Owner =:= BareJID);
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
        {running, is_member} ->
            SBareJID = escape(BareJID),
            case query_folder_info('id-name-type-owner', LServer, ["and project='", Project, "' and id='", Parent, "';"]) of
                [{ParentID, ParentName, ParentType, ParentOwner}] ->
                    case create_folder_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project) of
                        {allowed, SelfType} ->
                            F = fun() ->
                                {updated, 1} = ejabberd_odbc:sql_query_t(["insert into folder(type, name, creator, owner, parent, project) values('", SelfType, "', '",
                                    ejabberd_odbc:escape(Name), "', '", SBareJID, "', '", escape(ParentOwner), "', '", ParentID, "', '", Project, "');"]),
                                {selected, _, InsertFolder} = ejabberd_odbc:sql_query_t([query_folder_column(normal), "and id=last_insert_id();"]),
                                {ok, InsertFolder}
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
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                not_exist ->
                    case Parent of
                        <<>> -> %% %% mean create folder in self, privilige is ok.
                            F = fun() ->
                                ParentFolderStatus =
                                    case ejabberd_odbc:sql_query_t([query_folder_column(normal), "and project='", Project, "' and type='", ?TYPE_PERSON, "' and owner='", SBareJID, "';"]) of
                                        {selected, _, []} ->
                                            {updated, 1} = ejabberd_odbc:sql_query_t( ["insert into folder(type, name, creator, owner, parent, project) values('",
                                                ?TYPE_PERSON, "', '', '", SBareJID, "', '", SBareJID, "', '-1', '", Project, "');"]),
                                            {selected, _, [{ParentFolderID1, _Type, _Name, _Create, _Owner, _Create_at}]} =
                                                {selected, _, ParentFolder1} = ejabberd_odbc:sql_query_t([query_folder_column(normal), "and id=last_insert_id();"]),
                                            {ok, ParentFolderID1, ParentFolder1};
                                        {selected, _, ParentFolder2} ->
                                            [{ParentFolderID2, _Type, _Name, _Create, _Owner, _Create_at}] = ParentFolder2,
                                            {ok, ParentFolderID2, ParentFolder2};
                                        _ ->
                                            error
                                    end,
                                case ParentFolderStatus of
                                    {ok, ParentFolderID, ParentFolder} ->
                                        {updated, 1} = ejabberd_odbc:sql_query_t(["insert into folder(type, name, creator, owner, parent, project) values('",
                                             ?TYPE_PER_PUBLIC, "', '", escape(Name), "', '", SBareJID, "', '", SBareJID, "', '", ParentFolderID, "', '", Project, "');"]),
                                         {selected, _, InsertFolder} = ejabberd_odbc:sql_query_t([query_folder_column(normal), "and id=last_insert_id();"]),
                                         {ok, ParentFolderID, ParentFolder, InsertFolder};
                                    _ ->
                                        error
                                end
                            end,
                            case ejabberd_odbc:sql_transaction(LServer, F) of
                                {atomic, {ok, ParentFolderID, ParentFolder, InsertFolder}} ->
                                    ParentFolderJson = build_folder_result(ParentFolder),
                                    FolderJson = build_folder_result(InsertFolder),
                                    {ok, <<"[{\"parent\":\"-1\", \"folder\":", ParentFolderJson/binary,
                                        "}, {\"parent\":\"", ParentFolderID/binary, "\", \"folder\":", FolderJson/binary, "}]">>};
                                {atomic, {error, exist}} ->
                                    {error, ?ERR_BAD_REQUEST};
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        _ ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end
            end;
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

add_file_ex(LServer, BareJID, Parent, Name, UUID, Size, Project) ->
    case check_status(LServer, Project, BareJID) of
        {running, is_member} ->
            case check_add_file_uuid_valid(LServer, UUID) of
                invalid -> {error, ?AFT_ERR_INVALID_FILE_ID};
                valid ->
                    SBareJID = escape(BareJID),
                    case query_folder_info('id-name-type-owner', LServer, ["and project='", Project, "' and id='", Parent, "';"]) of
                        [{ParentID, ParentName, ParentType, ParentOwner}] ->
                            case create_file_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project) of
                                allowed ->
                                    F = fun() ->
                                        {updated, 1} = ejabberd_odbc:sql_query_t(["insert into file(uuid, name, size_byte, creator, version_count, folder) values('",
                                            escape(UUID), "', '", escape(Name), "', '", Size, "', '", SBareJID, "', '1', '", ParentID, "');"]),
                                        {selected, _, InsertFile} = ejabberd_odbc:sql_query_t([query_file_column(normal), "and id=last_insert_id();"]),
                                        {ok, InsertFile}
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
                                            {ok, <<"[{\"parent\":\"", ParentID/binary, "\", \"file\":", FileJson/binary, "}]">>};
                                        _ ->
                                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                                    end;
                                not_allowed ->
                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                            end;
                        not_exist ->
                            case Parent of
                                <<>> -> %% %% mean create FILE in self, privilige is ok.
                                    F = fun() ->
                                        ParentFolderStatus =
                                        case ejabberd_odbc:sql_query_t([query_folder_column(normal), "and project='", Project, "' and type='", ?TYPE_PERSON, "' and owner='", SBareJID, "';"]) of
                                            {selected, _, []} ->
                                                {updated, 1} = ejabberd_odbc:sql_query_t( ["insert into folder(type, name, creator, owner, parent, project) values('",
                                                    ?TYPE_PERSON, "', '', '", SBareJID, "', '", SBareJID, "', '-1', '", Project, "');"]),
                                                {selected, _, [{ParentFolderID1, _Type, _Name, _Create, _Owner, _Create_at}]} =
                                                    {selected, _, ParentFolder1} = ejabberd_odbc:sql_query_t([query_folder_column(normal), "and id=last_insert_id();"]),
                                                {ok, ParentFolderID1, ParentFolder1};
                                            {selected, _, ParentFolder2} ->
                                                [{ParentFolderID2, _Type, _Name, _Create, _Owner, _Create_at}] = ParentFolder2,
                                                {ok, ParentFolderID2, ParentFolder2};
                                            _ ->
                                                error
                                        end,
                                        case ParentFolderStatus of
                                            {ok, ParentFolderID, ParentFolder} ->
                                                {updated, 1} = ejabberd_odbc:sql_query_t(["insert into file(uuid, name, size_byte, creator, version_count, folder) values('", escape(UUID), "', '",
                                                    escape(Name), "', '", Size, "', '", escape(BareJID), "', '1', '", ParentFolderID, "');"]),
                                                {selected, _, InsertFile} = ejabberd_odbc:sql_query_t([query_file_column(normal), "and id=last_insert_id();"]),
                                                {ok, ParentFolderID, ParentFolder, InsertFile};
                                            _ ->
                                                error
                                        end
                                    end,
                                    case ejabberd_odbc:sql_transaction(LServer, F) of
                                        {atomic, {ok, ParentFolderID, ParentFolder, InsertFile}} ->
                                            ParentFolderJson = build_folder_result(ParentFolder),
                                            FileJson = build_file_result(InsertFile),
                                            {ok, <<"[{\"parent\":\"-1\", \"folder\":", ParentFolderJson/binary,
                                                "}, {\"parent\":\"", ParentFolderID/binary, "\", \"file\":", FileJson/binary, "}]">>};
                                        _ ->
                                            {error, ?ERR_INTERNAL_SERVER_ERROR}
                                    end;
                                _ ->
                                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                            end
                    end
            end;
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

delete_folder_ex(LServer, BareJID, ID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {running, is_member} ->
            case query_self_parent_info('type-name#id-name-type-owner', LServer, ID, <<"folder">>) of
                [{SelfType, SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    case delete_folder_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project) of
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
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

delete_file_ex(LServer, BareJID, ID, Project) ->
    case check_status(LServer, Project, BareJID) of
        {running, is_member} ->
            case query_self_parent_info('name#id-name-type-owner', LServer, ID, <<"file">>) of
                [{SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    case delete_file_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project) of
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
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

rename_file_or_folder_ex(LServer, BareJID, ID, Name, Project, Type) ->
    case check_status(LServer, Project, BareJID) of
        {running, is_member} ->
            case query_self_parent_info('creator-name#id-name-type-owner', LServer, ID, Type) of
                [{SelfUpdater, SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    IsAllowed = case Type  of
                                    <<"folder">> ->
                                        rename_folder_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, SelfUpdater, Project);
                                    <<"file">> ->
                                        rename_file_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, SelfUpdater, Project)
                                end,
                    case IsAllowed of
                        allowed ->
                            case ejabberd_odbc:sql_query(LServer, ["update ", Type ," set name='", escape(Name), "' where id='", ID, "';"]) of
                                {updated, 1} ->
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
                                _ ->
                                    {error, ?ERR_INTERNAL_SERVER_ERROR}
                            end;
                        not_allowed ->
                            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
                    end;
                _ ->
                    {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
            end;
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

share_ex(LServer, BareJID, ID, Users, Project) ->
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
            case query_self_parent_info('type#id-type-owner', LServer, ID, <<"folder">>) of
                [{SelfType, ParentID, ParentType, ParentOwner}] ->
                    case share_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, SelfType, Project) of
                        allowed ->
                            AfterType = case Users of
                                            false -> ?TYPE_PER_PRIVATE;
                                            _ ->
                                                if is_list(Users) ->
                                                    if length(Users) > 0 -> ?TYPE_PER_SHARE;
                                                        true -> ?TYPE_PER_PUBLIC
                                                    end;
                                                    true -> error
                                                end
                                        end,
                            case AfterType of
                                    error -> {error, ?ERR_BAD_REQUEST};
                                _ ->
                                    Temp = if
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
                                                      Temp =:= <<>> -> <<>>;
                                                      true -> <<"insert into share_users(folder, userjid) values", Temp/binary, ";">>
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
                                                {updated, _} =ejabberd_odbc:sql_query_t(InsertQuery),
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
        {running, is_member} ->
            case query_self_parent_info('name#id-name-type-owner', LServer, ID, Type) of
                [{SelfName, ParentID, ParentName, ParentType, ParentOwner}] ->
                    case query_folder_info('id-name-type-owner', LServer, ["and id='", DestFolder, "';"]) of
                        [{DestID, DestName, DestType, DestOwner}] ->
                            case move_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Type, 0, Project,
                                DestFolder, DestType, DestOwner) of
                                allowed ->
                                    SetObject = if
                                                    Type =:= <<"folder">> -> "parent";
                                                    true -> "folder"
                                                end,
                                    case ejabberd_odbc:sql_query(LServer, ["update ", Type, " set ", SetObject, "='", escape(DestFolder),
                                        "' where id='", ID, "';"]) of
                                        {updated, 1} ->
                                            Text = if
                                                       (ParentType =:= ?TYPE_PUBLIC) ->
                                                           <<ParentName/binary, ">", SelfName/binary>>;
                                                       (ParentType =:= ?TYPE_PUB_SUB) ->
                                                           [{PPName1}] = query_parent_info(name, LServer, ParentID, <<"folder">>),
                                                           <<PPName1/binary, ">", ParentName/binary, ">", SelfName/binary>>;
                                                       (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_PUBLIC) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
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
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

add_version_ex(LServer, BareJID, ID, UUID, Size, Project) ->
    case check_status(LServer, Project, BareJID) of
        {running, is_member} ->
            case query_self_parent_info('id-name-version_count#id-type-name-owner', LServer, ID, <<"file">>) of
                [{_SelfID, SelfName, VersionCount, ParentID, ParentType, ParentName, ParentOwner}] -> %% _SelfID, just only check file is exist or not, can check id is valied or not.
                    case add_version_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project) of
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
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

list_share_users_ex(LServer, BareJID, ID, Project) ->
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
            case query_folder_info('id-type-owner', LServer, ["and id='", ID, "';"]) of
                [{_ID, Type, Owner}] ->
                    case list_share_users_privilige(LServer, BareJID, 0, 0, 0, ID, Type, Owner, Project) of
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
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
            case query_self_parent_info('id-uuid-size_byte-creator-created_at#id-type-owner', LServer,  ID, <<"file">>) of
                [{SelfID, SelfUUID, SelfSize, SelfCreator, SelfTime, ParentID, ParentType, ParentOwner}] ->
                    case list_version_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, 0, 0, Project) of
                        allowed ->
                            case ejabberd_odbc:sql_query(LServer, ["select id, file, uuid, size_byte, creator, created_at from file_version where file='", ID, "';"]) of
                               %%{selected, _, {R_ID, R_File, R_UUID, R_Size, R_Updater, R_Time}} ->
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
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
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
                            case download_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner) of
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
                                now_allowed ->
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
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
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
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
            SelectPart = ["select file.id, file.name, file.location, file.deleted_at, folder.owner from file, folder ",
                "where file.status='0' and folder.project='", Project ,"'  and file.folder=folder.id and ( folder.owner='", escape(BareJID), "' "],
            AdminOrNot = case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                             {selected, _, [{BareJID}]} -> " or folder.owner='admin') ";
                             _ -> " ) "
                         end,
            OrderLimit = if
                             Before /= false ->
                                 AndID = if Before =:= <<>> -> "  "; true -> [" and file.deleted_at <'", integer_to_binary(Before), "' "] end,
                                [AndID, " order by file.deleted_at desc limit ", integer_to_binary(Count), ";"];
                            After /= false  ->
                                [" and file.deleted_at >'", integer_to_binary(After), "' order by file.deleted_at asc limit ", integer_to_binary(Count), ";"]
                         end,
            {selected, _, DeleteFiles} = ejabberd_odbc:sql_query(LServer, [SelectPart, AdminOrNot, OrderLimit]),
            FileJson = build_trash_file_result(DeleteFiles),
            ResultCount = integer_to_binary(length(DeleteFiles)),
            {ok, <<"{\"count\":\"", ResultCount/binary, "\", \"file\":", FileJson/binary, "}">>};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

clear_trash_ex(LServer, BareJID, Project) ->
    case odbc_organization:is_memeber(LServer, Project, BareJID) of
        true ->
            DeleteVersionFile= ["delete file_version.* from file_version, file, folder ",
                "where folder.project='", Project, "' and file.status='0' and file.folder=folder.id and file_version.file=file.id and ( folder.owner='", escape(BareJID), "' "],
            DeleteFile = ["delete file.* from file, folder ",
                "where folder.project='", Project, "' and file.status='0' and file.folder=folder.id and ( folder.owner='", escape(BareJID), "' "],
            DeleteFolder = ["delete from folder where project='", Project, "' and status='0' and ( owner='", escape(BareJID), "' "],
            AdminOrNot = case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                             {selected, _, [{BareJID}]} -> " or folder.owner='admin');";
                             _ -> " );"
                         end,

            F = fun() ->
                ejabberd_odbc:sql_query_t([DeleteVersionFile, AdminOrNot]),
                ejabberd_odbc:sql_query_t([DeleteFile, AdminOrNot]),
                ejabberd_odbc:sql_query_t([DeleteFolder, AdminOrNot]),
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
        {running, is_member} ->
            case query_parent_info('id-type-name-owner-2', LServer, ID, <<"file">>) of
                [{ParentID, ParentType, ParentName, ParentOwner}] ->
                    case query_folder_info('type-owner', LServer, ["and id='", DestFolder, "';"]) of
                        [{DestType, DestOwner}] ->
                            case recover_file_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner, Project,
                                DestFolder, DestType, DestOwner) of
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
        {finished, is_member} ->
            {error, ?AFT_ERR_ALLREADY_FINISHED};
        _ ->
            {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH}
    end.

get_normal_uuid_ex(LServer, BareJID, IDList, Project) ->
    case check_status(LServer, Project, BareJID) of
        {_, is_member} ->
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
        {_, is_member} ->
            SBareJID = escape(BareJID),
            SName = escape(Name),
            StartLine = integer_to_binary((binary_to_integer(Page) - 1) * binary_to_integer(Count)),
            Query = ["select f.id,f.uuid,f.name,f.size_byte,f.creator,f.version_count,f.folder,f.created_at from file as f, folder as fo ",
                "where (f.status='1' and fo.project='", Project, "' and (fo.owner='admin' or fo.owner='", SBareJID,
                "')) and ((f.folder=fo.id and f.folder='", Folder, "') or (f.folder=fo.id and fo.parent='", Folder,
                "')) and f.name like '%", SName, "%' order by f.created_at desc limit ", StartLine, ",", Count, ";"],
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
    Query = ["select p.status, count(ou.id) from organization_user as ou, organization as o, project as p where o.id=ou.organization and o.project='",
        Project, "' and ou.jid='", ejabberd_odbc:escape(JID),  "' and p.id='", Project, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, [{<<"0">>, <<"0">>}]} ->
            {finished, not_member};
        {selected, _, [{<<"0">>, <<"1">>}]} ->
            {finished, is_member};
        {selected, _, [{<<"1">>, <<"0">>}]} ->
            {running, not_member};
        {selected, _, [{<<"1">>, <<"1">>}]} ->
            {running, is_member};
        {selected, _, [{null, _}]} ->
            project_not_exists;
        _ ->
            error
    end.


%% ------------------------------------------------------------------
%% privilige function.
%% ------------------------------------------------------------------
%% TOFIX: build a mnesia ram privilige talbe may be can make code is more clear and maintenance is more effective???

recover_file_privilige(LServer, BareJID, _Folder, _Type, Owner, Project, _DestFolder, _DestType, DestOwner) ->
    case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
        {selected, _, [{BareJID}]} ->
            if
                ((Owner =:= <<"admin">>) or (Owner =:= BareJID)) and ((DestOwner =:= <<"admin">>) or (DestOwner =:= BareJID)) ->
                    allowed;
                true ->
                    not_allowed
            end;
        _ ->
            if
                (Owner =:= BareJID) and (DestOwner =:= BareJID) -> allowed;
                true -> not_allowed
            end
    end.

list_version_privilige(LServer, BareJID, ParentFolder, ParentType, ParentOwner, _SelfID, _SelfType, _Project) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) or (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_PUBLIC)->
            allowed;
        (ParentType =:= ?TYPE_PER_SHARE) ->
            case ejabberd_odbc:sql_query(LServer, ["select count(folder) from share_users where folder='", ParentFolder, "' and userjid='", BareJID, "';"]) of
                {selected, _, [{<<"1">>}]} ->
                    allowed;
                _ ->
                    if
                        BareJID =:= ParentOwner -> allowed;
                        true -> not_allowed
                    end
            end;
        (ParentType =:= ?TYPE_PER_PRIVATE) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end
    end.

list_share_users_privilige(LServer, BareJID, _Folder, _Type, _Owner, SelfID, SelfType, SelfOwner, _Project) ->
    if
        (SelfType =:= ?TYPE_PER_SHARE) ->
            %% ------------------- shoud check???
            case ejabberd_odbc:sql_query(LServer, ["select count(folder) from share_users where folder='", SelfID, "' and userjid='", BareJID, "';"]) of
                {selected, _, [{<<"1">>}]} ->
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

download_privilige(LServer, BareJID, ParentID, ParentType, ParentOwner) ->
    if
        (ParentType =:= ?TYPE_PUBLIC) or (ParentType =:= ?TYPE_PUB_SUB) or (ParentType =:= ?TYPE_PERSON) or (ParentType =:= ?TYPE_PER_PUBLIC) ->
            allowed;
        ParentType =:= ?TYPE_PER_SHARE ->
            if
                BareJID =:= ParentOwner -> allowed;
                true ->
                    case ejabberd_odbc:sql_query(LServer, ["select id from share_users where folder='", ParentID,
                        "' and userjid='", escape(BareJID), "';"]) of
                        {selected, _, [{_ID}]} -> allowed;
                        _ -> not_allowed
                    end
            end;
        ParentType =:= ?TYPE_PER_PRIVATE ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        true ->
            not_allowed
    end.

add_version_privilige(_LServer, BareJID, _Folder, Type, Owner, _Project) ->
    if
        (Type =:= ?TYPE_PUBLIC) or (Type =:= ?TYPE_PUB_SUB) ->
            allowed;
        (Type =:= ?TYPE_PERSON) or (Type =:= ?TYPE_PER_PUBLIC) or
            (Type =:= ?TYPE_PER_SHARE) or (Type =:= ?TYPE_PER_PRIVATE) ->
            if
                (BareJID =:= Owner) -> allowed;
                true -> not_allowed
            end
    end.

move_privilige(LServer, BareJID, Folder, Type, Owner,
    SelfType2, _SelfUpdater, Project, DestFolder, DestType, DestOwner) -> %% SelfType2 ---> <<"folder">> or <<"file">>
    if
        (Folder =:= DestFolder) -> not_allowed;
        true ->
            if
                (SelfType2 =:= <<"folder">>) ->
                    if
                        Type =:= ?TYPE_PUBLIC ->
                            case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                                {selected, _, [{BareJID}]} ->
                                    if
                                        (DestType =:= ?TYPE_PUBLIC) -> allowed;
                                        true -> not_allowed
                                    end;
                                _ ->
                                    not_allowed
                            end;
                        true -> not_allowed
                    end;
               true ->
                    if
                        (Type =:= ?TYPE_PUBLIC) or (Type =:= ?TYPE_PUB_SUB) ->
                            case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                                {selected, _, [{BareJID}]} ->
                                    %% admin move file from public folder to his personal folder is not allowed.
                                    if
                                        (DestType =:= ?TYPE_PUBLIC) or (DestType =:= ?TYPE_PUB_SUB) -> allowed;
                                        true -> not_allowed
                                    end;
                                _ ->
                                    not_allowed
                            end;
                        true ->
                           if
                               (BareJID =:= Owner) ->
                                   if
                                       (DestType =:= ?TYPE_PUBLIC) or (DestType =:= ?TYPE_PUB_SUB) ->
                                           allowed;
                                       (BareJID =:= DestOwner) ->
                                           allowed;
                                       true -> not_allowed
                                   end;
                               true ->
                                   not_allowed
                           end
                    end
            end
    end.

share_privilige(_LServer, BareJID, _Folder, ParentType, ParentOwner, SelfType, _Project) ->
    if
        (ParentType =:= ?TYPE_PERSON) and ( (SelfType =:= ?TYPE_PER_PUBLIC) or (SelfType =:= ?TYPE_PER_SHARE) or (SelfType =:= ?TYPE_PER_PRIVATE) ) ->
            if
                BareJID =:= ParentOwner -> allowed;
                true -> not_allowed
            end;
        true ->
            not_allowed
    end.

rename_folder_privilige(LServer, BareJID, _Folder, Type, Owner, SelfUpdater, Project) ->
    case Type of
        ?TYPE_PUBLIC ->
            case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                {selected, _, [{BareJID}]} ->
                    allowed;
                _ ->
                    if
                        BareJID =:= SelfUpdater -> allowed;
                        true -> not_allowed
                    end
            end;
        ?TYPE_PERSON ->
            if
                BareJID =:= Owner -> allowed;
                true -> not_allowed
            end;
        _ ->
            not_allowed
    end.

rename_file_privilige(LServer, BareJID, _Folder, Type, Owner, SelfUpdater, Project) ->
    if
        (Type =:= ?TYPE_PUBLIC) or (Type =:= ?TYPE_PUB_SUB) ->
            case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                {selected, _, [{BareJID}]} ->
                    allowed;
                _ ->
                    if
                        BareJID =:= SelfUpdater -> allowed;
                        true -> not_allowed
                    end
            end;
        (Type =:= ?TYPE_PERSON) or (Type =:= ?TYPE_PER_PUBLIC) or (Type =:= ?TYPE_PER_SHARE) or (Type =:= ?TYPE_PER_PRIVATE) ->
            if
                BareJID =:= Owner -> allowed;
                true -> not_allowed
            end;
        true ->
            not_allowed
    end.

delete_folder_privilige(LServer, BareJID, _Folder, Type, Owner, Project) ->
    case Type of
        ?TYPE_PUBLIC ->
            case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                {selected, _, [{BareJID}]} ->
                    allowed;
                _ ->
                    not_allowed
            end;
        ?TYPE_PERSON ->
            if
                BareJID =:= Owner -> allowed;
                true -> not_allowed
            end;
        _ ->
            not_allowed
    end.

delete_file_privilige(LServer, BareJID, _Folder, Type, Owner, Project) ->
    if
        (Type =:= ?TYPE_PUBLIC) or (Type =:= ?TYPE_PUB_SUB) ->
            case ejabberd_odbc:sql_query(LServer, ["select admin from project where id='", Project, "';"]) of
                {selected, _, [{BareJID}]} ->
                    allowed;
                _ ->
                    not_allowed
            end;
        (Type =:= ?TYPE_PERSON) or (Type =:= ?TYPE_PER_PUBLIC) or (Type =:= ?TYPE_PER_SHARE) or (Type =:= ?TYPE_PER_PRIVATE) ->
            if
                BareJID =:= Owner ->
                    allowed;
                true ->
                    not_allowed
            end;
        true ->
            not_allowed
    end.

create_folder_privilige(_LServer, BareJID, _Folder, Type, Owner, _Project) ->
    case Type of
        ?TYPE_PUBLIC ->
            {allowed, ?TYPE_PUB_SUB};
        ?TYPE_PERSON ->
            if
                BareJID =:= Owner ->
                    {allowed, ?TYPE_PER_PUBLIC};
                true ->
                    not_allowed
            end;
        _ ->
            not_allowed
    end.

create_file_privilige(LServer, BareJID, Folder, Type, Owner, _Project) ->
    if
        (Type =:= ?TYPE_PUBLIC) or (Type =:= ?TYPE_PUB_SUB) or (Type =:= ?TYPE_PERSON) or (Type =:= ?TYPE_PER_PUBLIC) ->
            allowed;
        (Type =:= ?TYPE_PER_SHARE) ->
            if
                BareJID =:= Owner ->
                    allowed;
                true ->
                    case ejabberd_odbc:sql_query(LServer,
                        ["select count(folder) from share_users where folder='", Folder, "' and userjid='", escape(BareJID), "';"]) of
                        {selected, _,[{<<"1">>}]} ->
                            allowed;
                        _ ->
                            not_allowed
                    end
            end;
        (Type =:= ?TYPE_PER_PRIVATE)->
            if
                BareJID =:= Owner ->
                    allowed;
                true ->
                    not_allowed
            end;
        true ->
            not_allowed
    end.


%% ------------------------------------------------------------------
%% other help function.
%% ------------------------------------------------------------------

%% build folder query sql.
folder_sql_query(LServer, Type, BareJID, Folder, Project, IsOwner) ->
    case Type of
        ?TYPE_ROOT ->
            Query = [query_folder_column(normal), "and parent ='-1' and (type='", ?TYPE_PUBLIC, "' or type='", ?TYPE_PERSON, "') and  project='", Project, "';"],
            [{folder, Query}];
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
                    Q1 = [query_folder_column(normal), "and parent='", Folder, "' and type='", ?TYPE_PER_PUBLIC, "';"],
                    Q2 = ["select id, type, name, creator, owner, created_at from folder, share_users where folder.status='1' and
                    folder.type='", ?TYPE_PER_SHARE, "' and share_users.userjid='", escape(BareJID),
                        "' and folder.id=share_users.folder and folder.project='", Project, "';"],
                    [{file, query_file_by_parent(Folder)}, {folder, Q1}, {folder, Q2}]
            end;
        ?TYPE_PER_PUBLIC ->
            [{file, query_file_by_parent(Folder)}];
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
        _ ->
            []
    end.

query_file_by_parent(Folder) ->
    ["select id, uuid, name, size_byte, creator, version_count, folder, created_at from file where folder='", Folder, "' and status='1';"].


build_folder_result(List) ->
    Result =
        lists:foldl(fun({ID, Type, Name, Creator, Owner, Time}, AccIn) ->
            AccIn1 = case AccIn of
                         <<>> -> <<>>;
                         _ -> <<AccIn/binary, ",">>
                     end,
            <<AccIn1/binary, "{\"id\":\"", ID/binary, "\", \"type\":\"", Type/binary, "\", \"name\":\"", Name/binary,
            "\", \"creator\":\"", Creator/binary, "\", \"owner\":\"", Owner/binary, "\", \"time\":\"", Time/binary, "\"}">>
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
                        "id, type, name, creator, owner, created_at";
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
                (ParentType =:= ?TYPE_PUBLIC) ->
                    {ok, ParentName};
                (ParentType =:= ?TYPE_PERSON) ->
                    {ok, <<"@">>};
                (ParentType =:= ?TYPE_PUB_SUB) or (ParentType =:= ?TYPE_PER_PUBLIC) or (ParentType =:= ?TYPE_PER_SHARE) or (ParentType =:= ?TYPE_PER_PRIVATE) ->
                    case ejabberd_odbc:sql_query(LServer, ["select f2.name from folder as f1, folder as f2 where f1.id='",
                        ParentID, "' and f1.parent=f2.id;"]) of
                        {selected, _, [{PPName}]} ->
                            if
                                (ParentType =:= ?TYPE_PUB_SUB) -> {ok, <<PPName/binary, ">", ParentName/binary>>};
                                true -> {ok, <<"@>", ParentName/binary>>}
                            end;
                        _ -> not_exist
                    end
            end;
       Type =:= <<"folder">> ->
           if
               (ParentType =:= ?TYPE_PUBLIC) ->
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

check_add_file_uuid_valid(LServer, UUID) ->
    case ejabberd_odbc:sql_query(LServer, ["select id from mms_file where id='", UUID, "'"]) of
        {selected,_,[{_ID}]} ->
            valid;
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
