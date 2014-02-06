open Graph
open Sexplib.Conv
open Topology

type topoType =
  | Persistent
  | Imperative

let ttype = ref Persistent
let infname = ref ""
let outfname = ref ""

module CachedNode = struct

  type t = { mutable hash : int option; data : Node.t }

  let hash (ch: t) : int = match ch with
    | { hash = Some n; _ } -> n
    | { hash = None; data = d } ->
      let n = Hashtbl.hash d in
      ch.hash <- Some n;
      n

  let equal (n1: t) (n2: t) = match n1,n2 with
    | { hash = Some n1'; _ }, { hash = Some n2'; _ } -> n1' = n2'
    | { hash = _; data = d1 }, { hash = _; data = d2 } -> d1 = d2

  let compare (n1: t) (n2: t) = match n1,n2 with
    | { hash = Some n1'; _ }, { hash = Some n2'; _ } -> Pervasives.compare n1' n2'
    | { hash = _; data = d1 }, { hash = _; data = d2 } -> Pervasives.compare d1 d2
end


module CachedEdge = struct

  type v = CachedNode.t
  type t = {
    srcport : portId;
    dstport : portId;
    cost : int64;
    capacity : int64;
  }

  type e = v * t * v
  let compare = Pervasives.compare
  let default = {
    srcport = 0L;
    dstport = 0L;
    cost = 1L;
    capacity = Int64.max_int
  }

end

module NodeTbl = Hashtbl.Make(CachedNode)

module type CACHED = sig
  module G : Sig.G with type V.t = CachedNode.t
  val make_from_topo : Topology.t -> G.t
end

module Make(C:CACHED) = struct
  include C

  let shortest_path t src dst len =
    let start = Unix.gettimeofday () in
    let distances = NodeTbl.create len in
    let visited = NodeTbl.create len in
    let previous =  NodeTbl.create len in
    let queue = Core.Std.Heap.create (fun (v,d) (v,d') -> compare d d') () in
    Core.Std.Heap.add queue (src,0);

    let init = Unix.gettimeofday () in
    let rec mk_path current =
      if current = src then [src] else
        let prev = NodeTbl.find previous current in
        prev::(mk_path prev) in

    let rec loop (current,distance) =
      if current = dst then ()
      else begin
        G.iter_succ (fun next ->
          if NodeTbl.mem visited next then ()
          else
            let next_dist = distance + 1 in
            let better =
              try next_dist < (NodeTbl.find distances next) with Not_found -> true
            in
            if better then begin
              NodeTbl.replace distances next next_dist;
              NodeTbl.replace previous next current;
              Core.Std.Heap.add queue (next,next_dist) end
        ) t current;
        NodeTbl.replace visited current 0;
        loop (Core.Std.Heap.pop_exn queue) end in

    loop (Core.Std.Heap.pop_exn queue);
    let find = Unix.gettimeofday () in
    let _ = mk_path dst in
    let gather = Unix.gettimeofday () in
    (init -. start, find -. init, gather -. find)

  let all_pairs (t:G.t) : (float * float * float * float) =
    let open CachedNode in
    let hosts = G.fold_vertex (fun v acc -> match v with
      | {hash =  _; data = Node.Host(_)} -> v::acc
      | _ -> acc
    ) t [] in
    let times = ref 0.0 in
    let itimes = ref 0.0 in
    let ftimes = ref 0.0 in
    let gtimes = ref 0.0 in
    let vert_num = G.nb_vertex t in
    let start = Unix.gettimeofday () in
    List.iter (fun src -> List.iter (fun dst ->
      if not (src = dst) then
        let each_start = Unix.gettimeofday () in
        let i,f,g = shortest_path t src dst vert_num in
        let each_stop = Unix.gettimeofday () in
        itimes := !itimes +. i;
        ftimes := !ftimes +. f;
        gtimes := !gtimes +. g;
        times := !times +. (each_stop -. each_start)
    ) hosts ) hosts;
    let stop = Unix.gettimeofday () in
    (stop -. start, !itimes, !ftimes, !gtimes)

end

module Persistent = Make(struct
  module G = Persistent.Digraph.ConcreteLabeled(CachedNode)(CachedEdge)

  let make_from_topo p =
    let g = G.empty in
    let open CachedNode in
    Topology.fold_edges_e (fun (s,l,d) g ->
      let s' = {hash = Some(Hashtbl.hash s); data = s} in
      let d' = {hash = Some(Hashtbl.hash d); data = d} in
      let open CachedEdge in
      let l' = {srcport = l.Link.srcport; dstport = l.Link.dstport;
                cost = l.Link.cost; capacity = l.Link.capacity} in
      G.add_edge_e g (s',l',d'))
      p g
end)

module Imperative = Make(struct
  module G = Imperative.Digraph.ConcreteLabeled(CachedNode)(CachedEdge)

  let make_from_topo p =
    let g = G.create ~size:(Topology.nb_edges p) () in
    let open CachedNode in
    Topology.iter_edges_e (fun (s,l,d) ->
      let s' = {hash = Some (Hashtbl.hash s); data = s} in
      let d' = {hash = Some (Hashtbl.hash d); data = d} in
      let open CachedEdge in
      let l' = {srcport = l.Link.srcport; dstport = l.Link.dstport;
                cost = l.Link.cost; capacity = l.Link.capacity} in
      G.add_edge_e g (s',l',d') ) p;
      g

end)

let arg_spec =
  [
    ("-p",
       Arg.Unit (fun () -> ttype := Persistent),
     "\tUse a persistent topology implementation")
    ; (
      "-i",
      Arg.Unit (fun () -> ttype := Imperative),
      "\tUse an imperative topology implementation")
    ; ("-o",
       Arg.String (fun s -> outfname := s ),
       "\tWrite topology to a file")
]

let usage = Printf.sprintf "usage: %s [--dot|--gml] filename -o filename [--dot|--mn]" Sys.argv.(0)


let _ =
  Arg.parse arg_spec (fun fn -> infname := fn) usage ;
  match !ttype with
    | Persistent ->
      Printf.printf "Running persistent version with cached node hashes\n%!";
      let g = from_dotfile !infname in
      let g' = Persistent.make_from_topo g in
      let total, i, f, g = Persistent.all_pairs g' in
      Printf.printf "All-pairs shortest path %f\n%!" total;
      Printf.printf "Init:%f Find:%f Gather:%f\n%!" i f g
    | Imperative ->
      Printf.printf "Running imperative version with cached node hashes\n%!";
      let g = from_dotfile !infname in
      let g' = Imperative.make_from_topo g in
      let total, i, f, g = Imperative.all_pairs g' in
      Printf.printf "All-pairs shortest path %f\n%!" total;
      Printf.printf "Init:%f Find:%f Gather:%f\n%!" i f g
