-module(odbc_push_service).

%% API
-export([add/2, remove/2, get_tokens_by_jid/2, get_roster_nick/3, get_group_nick/3, get_group_name/2]).


-include("jlib.hrl").
-include("mod_push_service.hrl").

add(LServer, #push_token{jid = UserJid, token = Token, type = PushType}) ->
    EJid = ejabberd_odbc:escape(UserJid),
    IType = integer_to_binary(PushType),
    Query = [<<"insert into push_service (jid, token, push_type) values ('">>,
        EJid, <<"','">>, ejabberd_odbc:escape(Token), <<"',">>,
        IType, <<") on duplicate key update jid = '">>, EJid,
        <<"', push_type = ">>, IType, <<", last_login = NOW();">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, _} ->
            ok;
        Error ->
            {error, Error}
    end.

remove(LServer, DeviceToken) ->
    Query = [<<"delete from push_service where token = '">>, ejabberd_odbc:escape(DeviceToken), <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {updated, 1} ->
            ok;
        Error ->
            {error, Error}
    end.

get_tokens_by_jid(LServer, UserJid) ->
    Query = [<<"select jid, token, push_type from push_service where jid = '">>, ejabberd_odbc:escape(UserJid), <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"jid">>, <<"token">>, <<"push_type">>], Rs} when is_list(Rs) ->
            PushTokenList = lists:map(fun result_to_record/1, Rs),
            {ok, PushTokenList};
        Error ->
            {error, Error}
    end.

result_to_record({Jid, Token, PushType}) ->
    #push_token{jid = Jid, token = Token, type = PushType}.

get_roster_nick(LServer, FromJid, ToUser) ->
    Query = [<<"select nick from rosterusers where jid = '">>, ejabberd_odbc:escape(FromJid),
        <<"' and username = '">>, ejabberd_odbc:escape(ToUser), <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"nick">>], [{Nickname}]} ->
            {ok, Nickname};
        Error ->
            {error, Error}
    end.

get_group_nick(LServer, FromJid, GroupId) ->
    Query = [<<"select nickname from groupuser where jid = '">>, ejabberd_odbc:escape(FromJid),
        <<"' and groupid = '">>, ejabberd_odbc:escape(GroupId), <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"nickname">>], [{Nickname}]} ->
            {ok, Nickname};
        Error ->
            {error, Error}
    end.

get_group_name(LServer, GroupId) ->
    Query = [<<"select name from groupinfo where groupid = '">>, ejabberd_odbc:escape(GroupId), <<"';">>],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"name">>], [{Name}]} ->
            {ok, Name};
        Error ->
            {error, Error}
    end.