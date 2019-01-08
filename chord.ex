
use Bitwise
defmodule HashGeneration do
  def getRandomHashCode(m) do
     getHash(:crypto.strong_rand_bytes(32) |> Base.url_encode64 |> binary_part(0,32),m)
  end
  def getHash(s,m) do
    a = :crypto.hash(:sha,s)
    first4Bytes = String.slice(a,0..3)
    binlist=:binary.bin_to_list(first4Bytes)
    mask = 0xffffffff >>> 32-m
    hash=Enum.reduce(binlist, 0, fn x, acc -> (acc <<< 8) + ( x &&& 0xff)  end)
    hash = hash &&& mask
    hash
  end
end

defmodule Proj3 do
  use GenServer
  def init(:ok) do
    {:ok,{-1,-1,0,[]}}
  end
  def start_node() do
    {:ok,pid}=GenServer.start_link(__MODULE__,:ok,[])
    pid
  end

  def updateHash(pid,m) do
    GenServer.cast(pid,{:calculateHash,m})
  end

  def handle_cast({:calculateHash,m},state) do
    {a,b,c,d}=state
    a=HashGeneration.getRandomHashCode(m)
    state={a,b,c,d}
    {:noreply,state}
  end

  def getKeyValue(pid) do
    GenServer.call(pid,:getKeyValue)
  end

  def handle_call(:getKeyValue,_from,state) do
    {h,p,s,f}=state
    {:reply,h,state}
  end

  def getHopCount(pid) do
    GenServer.call(pid, :getHopCount)
  end

  def handle_call(:getHopCount,_from,state) do
    {h,p,s,f}=state
    {:reply,s,state}
  end

  def simpleCast(pid) do
    GenServer.cast(pid,{:simpleCast})
  end

  def handle_cast({:simpleCast},state) do
    {h,p,s,f}=state
    {:noreply,state}
  end

  def updatePredecessor(pid,value) do
    GenServer.cast(pid,{:updatePredecessor,value})
  end

  def handle_cast({:updatePredecessor,value},state) do
    {h,p,s,f}=state
    p=value
    state={h,p,s,f}
    {:noreply,state}
  end

  def getSuccessor(pid) do
    GenServer.call(pid,{:getSuccessor})
  end
  def handle_call({:getSuccessor},state) do
    {h,p,s,f}=state
    {:reply,s}
  end

  def fetchFingerTable(pid) do
    GenServer.call(pid,{:fetchFingerTable})
  end
  def handle_call({:fetchFingerTable},state) do
    {h,p,s,f}=state
    {:reply,f}
  end
  def updateSuccesor(pid,value) do
    GenServer.cast(pid,{:updateSuccesor,value})
  end

  def handle_cast({:updateSuccesor,value},state) do
    {h,p,s,f}=state
    s=value
    f=f++value
    state={h,p,s,f}
    {:noreply,state}
  end

  def printPredecessor(pid) do
    GenServer.cast(pid,{:printPredecessor})
  end

  def handle_cast({:printPredecessor},state) do
    {h,p,s,f}=state
    {:noreply,state}
  end

  def sendRequests(pid,k,table,stop) do
    GenServer.cast(pid,{:sendRequests,k,table, stop})
  end

  def handle_cast({:sendRequests,k,table,stop},state) do
    {a,b,c,l}=state
    c=c+1
    c=
    if stop == 1 do
      c
    else
      if ((k > a and k < Enum.at(l,0)) or (k > a and k > Enum.at(l,0) and Enum.at(l,0) < a) or (k < a and k < Enum.at(l,0))) do
        c + 1
      else
        next_node_List=Enum.filter(0..length(l)-2, fn x ->
          (k >= Enum.at(l,x) and k <= Enum.at(l,x+1)) or (k > Enum.at(l,x) and Enum.at(l,x) > Enum.at(l,x+1)) or (k < Enum.at(l,x) and k < Enum.at(l,x+1) and Enum.at(l,x) > Enum.at(l,x+1))
        end)
        if length(next_node_List) == 0 do
          next_node = Enum.at(l,length(l)-1)
          xxx = elem(Enum.at(Enum.filter(table, fn x ->
            elem(x,0) == next_node
            end),0),1)

          sendRequests(xxx,k,table, 0)

        else
          next_node = Enum.at(l,Enum.at(next_node_List,0))
          xxx = elem(Enum.at(Enum.filter(table, fn x ->
            elem(x,0) == next_node
            end),0),1)
          sendRequests(xxx,k,table, 1)
        end
        c+1
      end
    end
    state = {a,b,c,l}
    {:noreply,state}

  end

  def print_finger_table(pid) do
    GenServer.cast(pid,{:printFingerList})
  end

  def updateFingerTableForAProcess(pid,val) do
    GenServer.cast(pid,{:updateFingerTableForAProcess,val})
  end

  def handle_cast({:updateFingerTableForAProcess,val},state) do
    {a,b,c,d}=state
    d =
    if val == nil do
      d
    else
      d++[val]
    end
    state={a,b,c,d}
    {:noreply,state}
  end

  def handle_cast({:printFingerList},state) do
    {h,p,s,f}=state
    {:noreply,state}
  end

  def handle_info({:runsomething},state) do
    schedule_work()
    {:noreply,state}
  end
  defp schedule_work() do
    Process.send_after(self(), :work, 2 * 1000) # In 2 seconds
  end

  def join(node_to_join,known_node) do
    if known_node != NULL do
      updatePredecessor(node_to_join,NULL)
      updateSuccesor(node_to_join,node_to_join)
      send(node_to_join,:runsomething)
    else

    end
  end

  def main() do
    arguments=System.argv()
    if Enum.count(arguments) != 2 do
      System.halt(1)
    end
    numNodes=Enum.at(arguments,0) |> String.to_integer()
    numRequests=Enum.at(arguments,1) |> String.to_integer()
    m = 30
    allNodes = Enum.map((1..numNodes),fn(_)->
      pid=start_node()
      pid
    end)
    table=[]
    Enum.each(allNodes,fn(x)->
      updateHash(x,m)
    end)
    table=table ++ Enum.map(allNodes,fn(x)->{getKeyValue(x),x} end )
    updateFingerTables(allNodes,table,m)

    Enum.each(allNodes,fn x -> print_finger_table(x) end)

    tasks_outer =
      Enum.map(allNodes, fn(x) ->
        Task.async(Proj3, :runParallel, [x,numRequests,table,m])
      end)
    Enum.each(tasks_outer, fn x -> Task.await(x, 60000) end)


    total_hops = Enum.reduce(allNodes,0, fn x,acc ->
      acc + getHopCount(x)
    end)

    average_hops = total_hops / (numNodes*numRequests)
    IO.inspect average_hops
  end
  def updateFingerTables(allNodes,table,m) do
    Enum.each(allNodes,fn(x)->
      getFingerTable(x,table,m)
    end)
  end
  def runParallel(x,numRequests,table,m) do
    tasks_inner =
      Enum.map(0..numRequests,fn t ->
          task = Task.async(Proj3,:runParallelRequests,[m,x,table])
          :timer.sleep(1000)
          task
     end)
     Enum.each(tasks_inner, fn x -> Task.await(x, 60000) end)
     tasks_inner
  end
  def runParallelRequests(m,x,table) do
    k=HashGeneration.getRandomHashCode(m)
    sendRequests(x,k,table,0)
  end

  def getFingerTable(x,table,m) do

    value=getKeyValue(x)
    Enum.each(0..m, fn(y)->
      zero_cross_over = 0
      first=Kernel.trunc(value + :math.pow(2,y))
      second=Kernel.trunc(:math.pow(2,m))
      zero_cross_over =
      if first > second do
        1
      else
        0
      end
      if zero_cross_over == 1 and rem(first, second) >= value do

        updateFingerTableForAProcess(x,nil)
      else
        nn=getSuccedingNeighbour(rem(first, second), table)

        nn =
        if nn == value do
          nil
        else
          nn
        end
        updateFingerTableForAProcess(x,nn)
      end
    end)
  end
  def getSuccedingNeighbour(k,table) do

    if k >= Enum.max(Enum.map(table,fn x -> elem(x,0) end)) do
      if k == Enum.max(Enum.map(table,fn x -> elem(x,0) end)) do
        k
      else
        Enum.min(Enum.map(table,fn x -> elem(x,0) end))
      end
    else
      greater_list=Enum.map(table,fn(x) ->
        if elem(x,0) >= k do
          elem(x,0)
        end
      end)
      Enum.min(greater_list)
    end
  end

end
Proj3.main()
