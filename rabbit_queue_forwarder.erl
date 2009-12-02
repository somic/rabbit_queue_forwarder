
% For experimental use only!

-module(rabbit_queue_forwarder).
-include_lib("stdlib/include/qlc.hrl").
-export([get_queue_pid/1, get_queue_pid/2, set_queue_pid/2, set_queue_pid/3]).
-export([qpid_lookup_server/0, forward_queue/2, show_table/1]).
-include("/usr/lib/erlang/lib/rabbitmq_server-1.6.0/include/rabbit.hrl").

% -----------------------------------------------------------

get_queue_pid(VHostPath, Queue) ->
  R = #resource{virtual_host = VHostPath, kind = queue, name = Queue},
  F = fun() ->
               mnesia:read(rabbit_durable_queue, R, read)
      end,
  {_, [Q]} = mnesia:transaction(F),
  Q#amqqueue.pid.
 
get_queue_pid(Queue) -> get_queue_pid(<<"/">>, Queue).


set_queue_pid(VHostPath, Queue, NewPid) ->
  R = #resource{virtual_host = VHostPath, kind = queue, name = Queue},
  F = fun() -> 
         [Q1] = mnesia:read(rabbit_queue, R, write),
         NewQ1 = Q1#amqqueue{pid=NewPid},
         mnesia:write(rabbit_queue, NewQ1, write),
         [Q] = mnesia:read(rabbit_durable_queue, R, write),
         NewQ = Q#amqqueue{pid=NewPid},
         mnesia:write(rabbit_durable_queue, NewQ, write)
       end,
  mnesia:transaction(F).

set_queue_pid(Queue, NewPid) -> set_queue_pid(<<"/">>, Queue, NewPid).

% -----------------------------------------------------------

qpid_lookup_server() ->
  register(qpid_lookup_server, self()),
  qpid_lookup_server_loop().

qpid_lookup_server_loop() ->
  receive
    { qpid_lookup, From, Q } ->
      io:format("Message from ~p for queue ~p~n", [From, Q]),
      Qpid = get_queue_pid(Q),
      io:format("Returning ~p~n", [Qpid]),
      From ! { qpid_lookup_resp, Q, Qpid },
      qpid_lookup_server_loop()
  end.

forward_queue(Queue, DestNode) ->
  { qpid_lookup_server, DestNode } ! { qpid_lookup, self(), Queue },
  receive
    { qpid_lookup_resp, Queue, NewPid } ->
      io:format("Received qpid_lookup_resp: NewPid=~p~n", [NewPid]),
      set_queue_pid(Queue, NewPid),
      {ok, show_table(rabbit_queue)}
  after 5000 ->
      {fail, timeout}
  end.

show_table(Tab) ->
  mnesia:transaction(
    fun() -> qlc:e(
        qlc:q([X || X <- mnesia:table(Tab)])
     ) end
  ).

%% vim: expandtab

