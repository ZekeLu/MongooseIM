-module(odbc_groupchat).

%% API
-export([
    create_group/2,
    add_members/3,
    get_groupinfo_by_groupid/2,
    create_and_add/3,
    get_members_by_groupid/2,
    get_groups_by_jid/2,
    get_groups_by_project/3,
    dismiss_group/3,
    complete_task/2,
    is_user_exists/2,
    is_user_in_group/3,
    is_user_own_group/3,
    is_in_project/3,
    set_groupname/3,
    set_avatar/3,
    set_nickname_in_group/4,
    remove_members/3]).

-include("jlib.hrl").
-include("organization.hrl").


-spec get_groupinfo_by_groupid(binary(), binary()) -> {ok, #group{}} | {error, _}.
get_groupinfo_by_groupid(LServer, GroupId) ->
    case ejabberd_odbc:sql_query(
        LServer,
        ["select name,owner,type,project,status,avatar from groupinfo where groupid = '", ejabberd_odbc:escape(GroupId), "';"]) of
        {selected, _, [{GroupName, GroupOwner, GroupType, Project, Status, Avatar}]} ->
            {ok, #group{groupid = GroupId, master = GroupOwner, groupname = GroupName,
                type = GroupType, project = Project, status = Status, avatar = Avatar}};
        Error ->
            {error, Error}
    end.


get_members_by_groupid(LServer, GroupId) ->
    case ejabberd_odbc:sql_query(
        LServer,
        [<<"select jid,nickname from groupuser"
        " where groupid = '">>, ejabberd_odbc:escape(GroupId), "';"]) of
        {selected, [<<"jid">>, <<"nickname">>], Rs} ->
            {ok, Rs};
        Error ->
            {error, Error}
    end.

-spec get_groups_by_jid(binary(), binary()) -> {ok, [#group{}]} | {error, _}.
get_groups_by_jid(LServer, UserJid) ->
    case ejabberd_odbc:sql_query(
        LServer,
        [<<"select groupinfo.groupid,groupinfo.name,groupinfo.owner,groupinfo.type,groupinfo.project,">>,
            <<"groupinfo.status,groupinfo.avatar,groupuser.private from groupinfo,groupuser where groupuser.jid = '">>,
            ejabberd_odbc:escape(UserJid), "' and groupinfo.groupid = groupuser.groupid;"]) of
        {selected, _, Rs} ->
            {ok, [#group{groupid = GroupId, master = GroupOwner, groupname = GroupName,
                type = GroupType, project = Project, private = Private, status = Status, avatar = Avatar}
                || {GroupId, GroupName, GroupOwner, GroupType, Project, Status, Avatar, Private} <- Rs]};
        Error ->
            {error, Error}
    end.

get_groups_by_project(LServer, Project, Type) ->
    Query = [<<"select groupid,name,owner,status,avatar from groupinfo where project = ">>,
        Project, " and type = ", Type, ";"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, R} ->
            {ok, [#group{groupid = GroupId, master = GroupOwner, groupname = GroupName,
                type = Type, project = Project, status = Status, avatar = Avatar}
                || {GroupId, GroupName, GroupOwner, Status, Avatar} <- R]}
    end.

-spec create_group(binary(), #group{}) -> {ok, binary()} | {error, _}.
create_group(LServer, Group) ->
    F = fun() ->
        Query =
            [<<"insert into groupinfo(name,owner,type,project,avatar) values('">>,
                ejabberd_odbc:escape(Group#group.groupname), "','",
                ejabberd_odbc:escape(Group#group.master), "',", Group#group.type, ",",
                case Group#group.project of
                    <<>> -> "NULL";
                    P -> P
                end, ",'", Group#group.avatar, ";);"],
        ejabberd_odbc:sql_query_t(Query),
        {selected, _, [{GroupId}]} = ejabberd_odbc:sql_query_t([<<"select last_insert_id();">>]),
        T = make_add_query([Group#group.master], [], GroupId),
        ejabberd_odbc:sql_query_t(T),
        GroupId
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, GroupId} ->
            {ok, GroupId};
        Error ->
            {error, Error}
    end.

add_members(LServer, GroupId, MembersList) ->
    F = fun() ->
        Allow = case ejabberd_odbc:sql_query_t([<<"select type,project from groupinfo where id = ">>, GroupId, ";"]) of
                    {selected, _, []} ->
                        error;
                    {selected, _, [{?TASK_GROUP, null}]} ->
                        error;
                    {selected, _, [{?TASK_GROUP, Project}]} ->
                        is_in_project(LServer, MembersList, Project);
                    _ ->
                        true
                end,
        case Allow of
            true ->
                AddQuery = make_add_query(MembersList, [], GroupId),
                MembersString = join_memberslist(MembersList),
                SelectQuery = [<<"select jid,nickname from groupuser where groupid ='">>, ejabberd_odbc:escape(GroupId),
                    <<"' and jid in ('">>, MembersString, <<"');">>],
                lists:foreach(fun(X) ->
                    ejabberd_odbc:sql_query_t(X)
                end, AddQuery),
                ejabberd_odbc:sql_query_t(SelectQuery);
            false ->
                not_valid;
            _ ->
                error
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {selected, [<<"jid">>, <<"nickname">>], Rs}} ->
            {ok, Rs};
        Error ->
            {error, Error}
    end.

-spec create_and_add(binary(), #group{}, [binary()]) -> {ok, _} | {error, _}.
create_and_add(LServer, Group, MembersList) ->
    F = fun() ->
        Query = [<<"insert into groupinfo(name,owner,type,project,avatar) values('">>,
            ejabberd_odbc:escape(Group#group.groupname), "','", ejabberd_odbc:escape(Group#group.master), "',",
            Group#group.type, ",",
            case Group#group.project of
                <<>> -> "NULL";
                P -> P
            end, ",'", Group#group.avatar, "');"],
        ejabberd_odbc:sql_query_t(Query),
        Result = ejabberd_odbc:sql_query_t([<<"select last_insert_id();">>]),
        {selected, _, [{RId}]} = Result,
        AllMembers = [Group#group.master | MembersList],
        QueryList = make_add_query(lists:usort(AllMembers), [], RId),
        lists:foreach(fun(X) ->
            ejabberd_odbc:sql_query_t(X)
        end, QueryList),
        ejabberd_odbc:sql_query_t([<<"select groupid,jid,nickname from groupuser where groupid ='">>,
            ejabberd_odbc:escape(RId), <<"' and jid <> '">>,
            ejabberd_odbc:escape(Group#group.master), <<"';">>])
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {selected, [<<"groupid">>, <<"jid">>, <<"nickname">>], Rs}} ->
            {ok, Rs};
        Error ->
            {error, Error}
    end.


-spec complete_task(binary(), binary()) -> ok | {error, _}.
complete_task(LServer, TaskId) ->
    Query = [<<"select type from groupinfo where id = ">>, TaskId, ";"],
    F = fun() ->
        case ejabberd_odbc:sql_query_t(Query) of
            {selected, _, []} ->
                not_exists;
            {selected, _, [{Type}]} ->
                case lists:member(Type, [?TASK_GROUP, ?EVENT_GROUP]) of
                    true ->
                        case ejabberd_odbc:sql_query_t([<<"update groupinfo set status = ">>,
                            ?STATUS_END, " where id = ", TaskId, ";"]) of
                            {updated, 1} ->
                                ok;
                            {updated, 0} ->
                                not_exists
                        end;
                    false ->
                        wrong_type
                end
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            ok;
        {atomic, not_exists} ->
            {error, not_exists};
        {atomic, wrong_type} ->
            {error, wrong_type};
        {atomic, Reason} ->
            {error, Reason}
    end.

set_groupname(LServer, GroupId, GroupName) ->
    T = ejabberd_odbc:sql_query(LServer, [<<"update groupinfo set name ='">>,
        ejabberd_odbc:escape(GroupName), <<"' where groupid = '">>,
        ejabberd_odbc:escape(GroupId), <<"';">>]),
    case T of
        {updated, 1} ->
            ok;
        Error ->
            {error, Error}
    end.

set_avatar(LServer, GroupId, Avatar) ->
    T = ejabberd_odbc:sql_query(LServer, [<<"update groupinfo set avatar ='">>,
        ejabberd_odbc:escape(Avatar), <<"' where groupid = '">>,
        ejabberd_odbc:escape(GroupId), <<"';">>]),
    case T of
        {updated, 1} ->
            ok;
        Error ->
            {error, Error}
    end.

set_nickname_in_group(LServer, GroupId, UserId, NickName) ->
    T = ejabberd_odbc:sql_query(LServer, [<<"update groupuser set nickname ='">>,
        ejabberd_odbc:escape(NickName), <<"' where groupid = '">>,
        ejabberd_odbc:escape(GroupId), <<"' and jid ='">>,
        ejabberd_odbc:escape(UserId), <<"';">>]),
    case T of
        {updated, 1} ->
            ok;
        Error ->
            {error, Error}
    end.

dismiss_group(LServer, GroupId, MembersInfoList) ->
    MembersString = join_memberslist([Jid || {Jid, _} <- MembersInfoList]),
    Query = [[<<"delete from groupinfo where groupid = '">>, ejabberd_odbc:escape(GroupId), <<"';">>],
        [<<"delete from groupuser where groupid ='">>, ejabberd_odbc:escape(GroupId),
            <<"' and jid in ('">>, MembersString, <<"');">>]
    ],
    T = ejabberd_odbc:sql_transaction(LServer, Query),
    case T of
        {atomic, _} ->
            ok;
        Error ->
            {error, Error}
    end.

remove_members(LServer, GroupId, MembersList) ->
    MembersString = join_memberslist(MembersList),
    F = fun() ->
        Result = ejabberd_odbc:sql_query_t([<<"select jid,nickname from groupuser where groupid ='">>,
            ejabberd_odbc:escape(GroupId), <<"' and jid in ('">>, MembersString, <<"');">>]),
        ejabberd_odbc:sql_query_t([<<"delete from groupuser where groupid ='">>, ejabberd_odbc:escape(GroupId),
            <<"' and jid in ('">>, MembersString, <<"');">>]),
        Result
    end,
    T = ejabberd_odbc:sql_transaction(LServer, F),
    case T of
        {atomic, {selected, [<<"jid">>, <<"nickname">>], Rs}} ->
            {ok, Rs};
        Error ->
            {error, Error}
    end.


%% ====================================
%% permission
%% =====================================

is_user_exists(LServer, UserName) ->
    T = ejabberd_odbc:sql_query(LServer, [<<"select count(username) from users where username ='">>,
        ejabberd_odbc:escape(UserName), <<"';">>]),
    case T of
        {selected, _, [{<<"0">>}]} ->
            false;
        {selected, _, [{<<"1">>}]} ->
            true;
        Error ->
            {error, Error}
    end.

is_user_in_group(LServer, UserJid, GroupId) ->
    T = ejabberd_odbc:sql_query(LServer, [<<"select count(id) from groupuser where groupid ='">>,
        ejabberd_odbc:escape(GroupId), <<"' and jid = '">>,
        ejabberd_odbc:escape(UserJid), <<"';">>]),
    case T of
        {selected, _, [{<<"0">>}]} ->
            false;
        {selected, _, [{<<"1">>}]} ->
            true;
        Error ->
            {error, Error}
    end.

is_user_own_group(LServer, UserJid, GroupId) ->
    T = ejabberd_odbc:sql_query(LServer, [<<"select count(groupid) from groupinfo where groupid ='">>,
        ejabberd_odbc:escape(GroupId), <<"' and owner = '">>,
        ejabberd_odbc:escape(UserJid), <<"';">>]),
    case T of
        {selected, _, [{<<"0">>}]} ->
            false;
        {selected, _, [{<<"1">>}]} ->
            true;
        Error ->
            {error, Error}
    end.

-spec is_in_project(binary(), [binary()], binary()) -> true | {false, [binary()]} | {error, _}.
is_in_project(LServer, JidList, Project) ->
    Jids = binary_join(JidList, <<"','">>),
    Query = [<<"select jid from organization_user where jid in ('">>, Jids, "') and project = ", Project, ";"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, _, R} ->
            case lists:subtract(JidList, [X || {X} <- R]) of
                [] ->
                    true;
                L ->
                    {false, L}
            end;
        Reason ->
            {error, Reason}
    end.


%% ==============================
%% helper
%% =============================

make_add_query([H | T], Result, GroupID) ->
    #jid{luser = LUser} = jlib:binary_to_jid(H),
    Query =
        [<<"insert into groupuser(groupid, jid, nickname) select '">>
            , ejabberd_odbc:escape(GroupID), "', '", ejabberd_odbc:escape(H),
            <<"', nickname from vcard_search where username ='">>, ejabberd_odbc:escape(LUser), <<"';">>],
    make_add_query(T, [Query | Result], GroupID);
make_add_query([], Result, _GroupID) ->
    Result.

join_memberslist(MembersList) ->
    binary_join(MembersList, <<"','">>).


-spec binary_join([binary()], binary()) -> binary().
binary_join([], _Sep) ->
    <<>>;
binary_join([Part], _Sep) ->
    Part;
binary_join(List, Sep) ->
    lists:foldr(fun(A, B) ->
        if
            bit_size(B) > 0 -> <<A/binary, Sep/binary, B/binary>>;
            true -> A
        end
    end, <<>>, List).