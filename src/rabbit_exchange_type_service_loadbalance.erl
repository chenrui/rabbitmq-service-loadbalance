%%%-------------------------------------------------------------------
%%% @author chenrui
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. 三月 2016 下午2:07
%%%-------------------------------------------------------------------
-module(rabbit_exchange_type_service_loadbalance).
-author("chenrui").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_exchange_type).

%% API
-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
        create/2, delete/3, policy_changed/2,
        add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

-rabbit_boot_step(
     {rabbit_sharding_exchange_type_modulus_hash_registry,
      [{description, "exchange type x-service-loadbalance: registry"},
        {mfa,         {rabbit_registry, register,
                        [exchange, <<"x-service-loadbalance">>, ?MODULE]}},
        {cleanup, {rabbit_registry, unregister,
                    [exchange, <<"x-service-loadbalance">>]}},
        {requires,    rabbit_registry},
        {enables,     kernel_ready}]}).

-define(PHASH2_RANGE, 134217728). %% 2^27

description() ->
    [{description, <<"Header Service Loadbalance Exchange">>}].

serialise_events() -> false.

route(#exchange{name = Name},
      #delivery{message = #basic_message{content = Content}}) ->
    Headers = case (Content#content.properties)#'P_basic'.headers of
                undefined -> [];
                H         -> rabbit_misc:sort_field_table(H)
              end,
    ServiceName = parse_sesrvice(rabbit_misc:table_lookup(Headers, <<"service">>)),
    %% 查找绑定到exchange上的queue
    Qs = rabbit_router:match_routing_key(Name, [ServiceName]),
    case length(Qs) of
        0 -> [];
        N -> [service_loadbalance(N, Qs)]
    end.

parse_sesrvice({longstr, ServiceName}) -> ServiceName;
parse_sesrvice(_) -> undefined.

service_loadbalance(N, Qs) ->
    %% messages不为0时路由到messages个数最少的queue,
    %% 否则随机分配
    List = get_msg_ready_num(Qs),
    SortedList = lists:keysort(2, List),
    {_, {_, Num}} = lists:last(SortedList),
    case Num of
        0 -> lists:nth(hash_mod(random:uniform(), N), Qs);
        _ ->
            [{Q, _} | _] = SortedList,
            Q
    end.

get_msg_ready_num(Qs) ->
    foreach_queue(fun(Q) ->
                      [Info|_] = rabbit_amqqueue:with(
                          Q,
                          fun(Queue) ->
                              rabbit_amqqueue:info(Queue, [messages])
                          end),
                      {Q, Info}
                  end, Qs).

foreach_queue(F, QNames) ->
    [F(Q) || Q <- QNames].

hash_mod(Key, N) ->
    M = erlang:phash2(Key, ?PHASH2_RANGE) rem N,
    M + 1. %% erlang lists are 1..N indexed.


validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
