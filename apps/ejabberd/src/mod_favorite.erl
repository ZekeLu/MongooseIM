-module(mod_favorite).

-behaviour(gen_mod).

%% API
-export([start/2, stop/1, process_iq/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(NS_AFT_FAVORITE, <<"aft:favorite">>).


-record(favorite, {id :: integer() | binary(),
                   jid :: binary(),
                   fromjid :: binary(),
                   title :: binary(),
                   type :: integer() | binary(),
                   content :: binary(),
                   tag :: binary()
                  }).

start(Host, _Opts) ->
    gen_iq_handler:add_iq_handler(ejabberd_local, Host, ?NS_AFT_FAVORITE,
                                  ?MODULE, process_iq, no_queue),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_AFT_FAVORITE,
                                  ?MODULE, process_iq, no_queue).

stop(Host) ->
    gen_iq_handler:remove_iq_handler(ejabberd_local, Host, ?NS_AFT_FAVORITE),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_AFT_FAVORITE).

process_iq(From, To, #iq{xmlns = ?NS_AFT_FAVORITE, sub_el = SubEl} = IQ) ->
    case xml:get_tag_attr_s(<<"type">>, SubEl) of
        <<"list">> ->
            list(From, To, IQ);
         <<"get_tag">> ->
             get_tag(From, To, IQ);
         <<"list_tags">> ->
             list_tags(From, To, IQ);
         <<"add">> ->
             add(From, To, IQ);
         <<"delete">> ->
             delete(From, To, IQ);
         <<"add_tag">> ->
             add_tag(From, To, IQ);
        <<"delete_tag">> ->
            delete_tag(From, To, IQ);
        _ ->
            IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}
    end;
process_iq(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


%% ------------------------------------------------------------------
%% higher function. called by process_iq.
%% ------------------------------------------------------------------

list(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,

    case list_ex(S, BaseJID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                  sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"list">>}],
                                        children = [{xmlcdata, Result}]}]}
    end;
list(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


get_tag(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,

    case get_tag_ex(S, BaseJID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"get_tag">>}],
                    children = [{xmlcdata, Result}]}]}
    end;
get_tag(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


list_tags(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = get, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,

    case list_tags_ex(S, BaseJID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"list_tags">>}],
                    children = [{xmlcdata, Result}]}]}
    end;
list_tags(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

add(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, Title} = lists:keyfind(<<"title">>, 1, Data),
    {_, JID} = lists:keyfind(<<"from">>, 1, Data),
    {_, Type} = lists:keyfind(<<"type">>, 1, Data),
    {_, Content} = lists:keyfind(<<"content">>, 1, Data),

    case add_ex(S, BaseJID, Title, JID, Type, Content) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"add">>}],
                    children = [{xmlcdata, Result}]}]}
    end;
add(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

delete(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),

    case delete_ex(S, BaseJID, ID) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"delete">>}],
                    children = [{xmlcdata, Result}]}]}
    end;
delete(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

add_tag(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, Tag} = lists:keyfind(<<"tag">>, 1, Data),

    case add_tag_ex(S, BaseJID, ID, Tag) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"add_tag">>}],
                    children = [{xmlcdata, Result}]}]}
    end;
add_tag(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.

delete_tag(From, _To, #iq{xmlns = ?NS_AFT_FAVORITE, type = set, sub_el = SubEl} = IQ) ->
    #jid{user = U, server = S} = From,
    BaseJID = <<U/binary, "@", S/binary>>,
    {struct, Data} = mochijson2:decode(xml:get_tag_cdata(SubEl)),
    {_, ID} = lists:keyfind(<<"id">>, 1, Data),
    {_, Tag} = lists:keyfind(<<"tag">>, 1, Data),

    case delete_tag_ex(S, BaseJID, ID, Tag) of
        {error, Error} ->
            IQ#iq{type = error, sub_el = [SubEl, Error]};
        {ok, Result} ->
            IQ#iq{type = result,
                sub_el = [SubEl#xmlel{attrs = [{<<"xmlns">>, ?NS_AFT_FAVORITE}, {<<"type">>, <<"delete_tag">>}],
                    children = [{xmlcdata, Result}]}]}
    end;
delete_tag(_, _, IQ) ->
    IQ#iq{type = error, sub_el = [?ERR_BAD_REQUEST]}.


%% ------------------------------------------------------------------
%% bridge betwwen odbc and higer function.
%% ------------------------------------------------------------------

tuplelist_to_list( [], Result) ->
    lists:reverse(Result);
tuplelist_to_list([{R} | T ], Result )->
    NR = [ R | Result ],
    tuplelist_to_list( T, NR ).

build_list([], [], Result) ->
    F = mochijson2:encoder([{utf8, true}]),
    iolist_to_binary( F(Result) );
build_list([H1 | T1], [H2 | T2], Result) ->
    NH2 = tuplelist_to_list(H2, []),
    {ID, _JID, FromJID,  Title, Type, Content, Tag} = H1,
    NewResult = [{struct, [{<<"id">>, ID}, {<<"from">>, FromJID}, {<<"title">>, Title}, {<<"type">>, Type},
        {<<"content">>, Content}, {<<"time_tag">>, Tag}, {<<"tags">>, NH2}]} | Result ],
    build_list( T1, T2, NewResult ).

build_tags([], [], Result) ->
    F = mochijson2:encoder([{utf8, true}]),
    iolist_to_binary( F(Result) );
build_tags([H1 | T1], [H2 | T2], Result) ->
    {ID} = H1,
    NH2 = tuplelist_to_list(H2, []),
    NewResult = [{struct, [{<<"id">>, ID},  {<<"tags">>, NH2}]} | Result ],
    build_tags( T1, T2, NewResult ).

list_ex(LServer, BaseJID) ->
    Query = ["select id, jid, fromjid, title, type, content, tag from favorite where jid='", BaseJID, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"id">>, <<"jid">>, <<"fromjid">>, <<"title">>, <<"type">>, <<"content">>, <<"tag">>], Result} ->
            TagList = lists:map(fun(E) ->
                {ID,_,_,_,_,_,_} = E,
               {selected, [<<"tag">>], R } = ejabberd_odbc:sql_query(LServer, ["select tag from favorite_tag where fid='", ID, "';"]),
                R
                end,
               Result),
            {ok, build_list( Result, TagList, [])};
        ErrorReason ->
            ErrorReason
    end.

get_tag_ex(LServer, BaseJID) ->
    Query = ["select tag from favorite_change where jid='", BaseJID, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"tag">>], [{Result}]} ->
            {ok, << "{\"tag\":\"", Result/binary, "\"}" >>};
        {selected, _, []} ->
            {ok, << "{\"tag\":\"\"}" >>};
        ErrorReason ->
            ErrorReason
    end.

list_tags_ex(LServer, BaseJID) ->
    Query = ["select id from favorite where jid='", BaseJID, "';"],
    case ejabberd_odbc:sql_query(LServer, Query) of
        {selected, [<<"id">>], Result} ->
            TagList = lists:map(fun({ID}) ->
                {selected, [<<"tag">>], R } = ejabberd_odbc:sql_query(LServer, ["select tag from favorite_tag where fid='", ID, "';"]),
                R
            end,
                Result),
            {ok, build_tags( Result, TagList, [])};
        ErrorReason ->
            ErrorReason
    end.


add_ex(LServer, BaseJID, Title, JID, Type, Content) ->
    TimeString = now_random(),
    Query = ["insert into favorite (jid, fromjid, title, type, content, tag) values('", BaseJID, "', '", JID  ,"', '",
        ejabberd_odbc:escape(Title), "', '", Type, "', '", ejabberd_odbc:escape(Content), "', '", TimeString , "');"],
    F = fun() ->
        case ejabberd_odbc:sql_query_t(Query) of
            {updated, 1} ->
                {selected, [<<"id">>], [{ID}]} = ejabberd_odbc:sql_query_t(["select last_insert_id() as id"]),
                ejabberd_odbc:sql_query_t(["insert into favorite_change values('", BaseJID, "', '", TimeString, "') on
                duplicate key update tag='", TimeString, "';"]),
                {ok, ID};
            Reason ->
                Reason
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, {ok, ID}} ->
            Fun = mochijson2:encoder([{utf8, true}]),
            JsonObject = {struct, [{<<"id">>, ID}, {<<"from">>, JID}, {<<"title">>, Title}, {<<"type">>, Type},
                {<<"content">>, Content}, {<<"time_tag">>, TimeString}, {<<"tags">>, []}]},
            {ok, iolist_to_binary( Fun(JsonObject) )};
        ErrorReason ->
            ErrorReason
    end.

delete_ex(LServer, BaseJID, ID ) ->
    TimeString = now_random(),
    Query1 = ["delete from favorite where id='", ID, "' and jid='", BaseJID, "';"],
    Query2 = ["delete from favorite_tag where fid='", ID, "';"],
    Query3 = ["update favorite_change set tag='", TimeString, "' where jid='", BaseJID, "';"],
    F = fun() ->
        case ejabberd_odbc:sql_query_t(Query1) of
            {updated, 1} ->
                ejabberd_odbc:sql_query_t(Query2),
                ejabberd_odbc:sql_query_t(Query3),
                ok;
            Reason ->
                Reason
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok} ->
            {ok, <<"{\"id\":\"", ID/binary, "\", \"time_tag\":\"", TimeString/binary, "\"}">>};
        ErrorReason ->
            ErrorReason
    end.

add_tag_ex(LServer, BaseJID, ID, Tag) ->
    TimeString = now_random(),
    F = fun() ->
        Query = ["select id from favorite where id='", ID, "' and jid='", BaseJID, "';"],
        case ejabberd_odbc:sql_query_t(Query) of
            {selected, [<<"id">>], []} ->
                {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
            {selected, [<<"id">>], [{_ID}]} ->
                Query1 = ["insert into favorite_tag (fid, tag) values('", ID, "', '", ejabberd_odbc:escape(Tag), "');"],
                case ejabberd_odbc:sql_query_t(Query1) of
                    {updated, 1} ->
                        ejabberd_odbc:sql_query_t(["update favorite set tag='", TimeString, "' where id='", ID, "';"]),
                        ok;
                    Reason1 ->
                        Reason1
                end;
            Reason2 ->
                Reason2
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok}  ->
            {ok, <<"{\"id\":\"", ID/binary, "\", \"tag\":\"", Tag/binary, "\", \"time_tag\":\"", TimeString/binary, "\"}">>};
        ErrorReason ->
            ErrorReason
    end.

delete_tag_ex(LServer, BaseJID, ID, Tag) ->
    TimeString = now_random(),
    F = fun() ->
        Query = ["select f.id, ft.id as tag_id from favorite f, favorite_tag ft where f.id='", ID, "' and f.jid='", BaseJID,
            "' and ft.fid='", ID, "' and ft.tag='", ejabberd_odbc:escape(Tag),"';"],
        case ejabberd_odbc:sql_query_t(Query) of
            {selected, [<<"id">>, <<"tag_id">>], []} ->
                {error, ?AFT_ERR_PRIVILEGE_NOT_ENOUGH};
            {selected, [<<"id">>, <<"tag_id">>], [{ID, TagID}]} ->
                Query1 = ["delete from favorite_tag where id='", TagID, "';"],
                case ejabberd_odbc:sql_query_t(Query1) of
                    {updated, 1} ->
                        ejabberd_odbc:sql_query_t(["update favorite set tag='", TimeString, "' where id='", ID ,"';"]),
                        ok;
                    Reason1 ->
                        Reason1
                end;
            Reason2 ->
                Reason2
        end
    end,
    case ejabberd_odbc:sql_transaction(LServer, F) of
        {atomic, ok}  ->
            {ok, <<"{\"id\":\"", Tag/binary, "\", \"time_tag\":\"", TimeString/binary, "\"}">>};
        ErrorReason ->
            ErrorReason
    end.

%% ------------------------------------------------------------------
%% helper function.
%% ------------------------------------------------------------------

now_random() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    Ran = random:uniform(9999),
    list_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ@~4..0w",
        [Year, Month, Day, Hour, Minute, Second, Ran])).



