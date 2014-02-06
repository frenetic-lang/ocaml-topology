open Graph
open Sexplib.Conv
open Topology

type topoType =
  | Persistent
  | Imperative
  | CachedPersistent
  | CachedImperative

let ttype = ref Persistent
let infname = ref ""
let outfname = ref ""

module Weight = struct
  open Link
  type t = Int64.t
  type label = Link.t
  let weight l = l.cost
  let compare = Int64.compare
  let add = Int64.add
  let zero = Int64.zero
end

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

let generic_sp t src dst len neighbors =
  let start = Unix.gettimeofday () in
  let distances = Hashtbl.create len in
  let visited = Hashtbl.create len in
  let previous =  Hashtbl.create len in
  let queue = Core.Std.Heap.create (fun (v,d) (v,d') -> compare d d') () in
  Core.Std.Heap.add queue (src,0);

  let init = Unix.gettimeofday () in
  let rec mk_path current =
    if current = src then [src] else
      let prev = Hashtbl.find previous current in
      prev::(mk_path prev) in

  let rec loop (current,distance) =
    if current = dst then ()
    else begin
      neighbors (fun next ->
        if Hashtbl.mem visited next then ()
        else
          let next_dist = distance + 1 in
          let better =
            try next_dist < (Hashtbl.find distances next) with Not_found -> true
          in
          if better then begin
            Hashtbl.replace distances next next_dist;
            Hashtbl.replace previous next current;
            Core.Std.Heap.add queue (next,next_dist) end
      ) t current;
      Hashtbl.replace visited current 0;
      loop (Core.Std.Heap.pop_exn queue) end in

  loop (Core.Std.Heap.pop_exn queue);
  let find = Unix.gettimeofday () in
  let p = mk_path dst in
  let gather = Unix.gettimeofday () in
  (init -. start, find -. init, gather -. find)

module CachedPers = struct
  module G = Persistent.Digraph.ConcreteLabeled(CachedNode)(CachedEdge)
  include G

  let make_from_persistent p =
    let g = empty in
    let open CachedNode in
    Topology.fold_edges_e (fun (s,l,d) g ->
      let s' = {hash = None; data = s} in
      let d' = {hash = None; data = d} in
      let open CachedEdge in
      let l' = {srcport = l.Link.srcport; dstport = l.Link.dstport;
                cost = l.Link.cost; capacity = l.Link.capacity} in
      add_edge_e g (s',l',d'))
      p g

  let shortest_path t src dst = generic_sp t src dst (nb_vertex t) iter_succ

end

module CachedImp = struct
  module G = Imperative.Digraph.ConcreteLabeled(CachedNode)(CachedEdge)
  include G
  (* module Dij = Path.Dijkstra(G)(Weight) *)

  let make_from_persistent p =
    let g = create ~size:(Topology.nb_edges p) () in
    let open CachedNode in
    Topology.iter_edges_e (fun (s,l,d) ->
      let s' = {hash = None; data = s} in
      let d' = {hash = None; data = d} in
      let open CachedEdge in
      let l' = {srcport = l.Link.srcport; dstport = l.Link.dstport;
                cost = l.Link.cost; capacity = l.Link.capacity} in
      add_edge_e g (s',l',d') ) p;
      g

  let shortest_path t src dst = generic_sp t src dst (nb_vertex t) iter_succ

end

module Pers = struct
  include Topology
  let shortest_path t src dst = generic_sp t src dst (nb_vertex t) iter_succ
end

module Imp = struct
  module G = Imperative.Digraph.ConcreteBidirectionalLabeled(Node)(Link)
  include G
  module Dij = Path.Dijkstra(G)(Weight)

  let make_from_persistent p =
    let g = create ~size:(Topology.nb_edges p) () in
    Topology.iter_edges_e (add_edge_e g) p;
    g

  let shortest_path t src dst = generic_sp t src dst (nb_vertex t) iter_succ

end

let arg_spec =
  [
    ("-p",
       Arg.Unit (fun () -> ttype := Persistent),
     "\tUse a persistent topology implementation")
    ; (
      "-i",
      Arg.Unit (fun () -> ttype := Imperative),
      "\tUse an imperative topology implementation")
    ; (
      "-cp",
      Arg.Unit (fun () -> ttype := CachedPersistent),
      "\tUse an persistent topology implementation with cached node hashes")
    ; (
      "-ci",
      Arg.Unit (fun () -> ttype := CachedImperative),
      "\tUse an imperative topology implementation with cached node hashes")
    ; ("-o",
       Arg.String (fun s -> outfname := s ),
       "\tWrite topology to a file")
]

let usage = Printf.sprintf "usage: %s [--dot|--gml] filename -o filename [--dot|--mn]" Sys.argv.(0)

let persistent_dijkstra (t:Pers.t) : (int * float * int) =
  let hosts = Pers.get_hosts t in
  let times = ref 0 in
  let start = Unix.gettimeofday () in
  List.iter (fun src -> List.iter (fun dst ->
    if not (src = dst) then let _ = Pers.shortest_path t src dst in
                            times := !times +1
  ) hosts ) hosts;
  let stop = Unix.gettimeofday () in
  (List.length hosts, stop -. start, !times)

let cached_persistent_dijkstra (t:CachedPers.t) : (int * float * int) =
  let open CachedNode in
  let hosts = CachedPers.fold_vertex (fun v acc -> match v with
      | {hash =  _; data = Node.Host(_)} -> v::acc
      | _ -> acc
    ) t [] in
  let times = ref 0 in
  let start = Unix.gettimeofday () in
  List.iter (fun src -> List.iter (fun dst ->
    if not (src = dst) then let _ = CachedPers.shortest_path t src dst in
                            times := !times +1
  ) hosts ) hosts;
  let stop = Unix.gettimeofday () in
  (List.length hosts, stop -. start, !times)

let cached_imperative_dijkstra (t:CachedImp.t)  =
  let open CachedNode in
  let hosts = CachedImp.fold_vertex (fun v acc -> match v with
      | {hash =  _; data = Node.Host(_)} -> v::acc
      | _ -> acc
    ) t [] in
  let times = ref 0.0 in
  let itimes = ref 0.0 in
  let ftimes = ref 0.0 in
  let gtimes = ref 0.0 in
  let vert_num = CachedImp.nb_vertex t in
  let start = Unix.gettimeofday () in
  List.iter (fun src -> List.iter (fun dst ->
    if not (src = dst) then
      let each_start = Unix.gettimeofday () in
      let i,f,g = generic_sp t src dst vert_num CachedImp.iter_succ in
      let each_stop = Unix.gettimeofday () in
      itimes := !itimes +. i;
      ftimes := !ftimes +. f;
      gtimes := !gtimes +. g;
      times := !times +. (each_stop -. each_start)
  ) hosts ) hosts;
  let stop = Unix.gettimeofday () in
  (stop -. start, !itimes, !ftimes, !gtimes)

let imperative_dijkstra (t:Imp.t) : (int * float * int) =
  let hosts = Imp.fold_vertex (fun v acc -> match v with
      | Node.Host(_) -> v::acc
      | _ -> acc
    ) t [] in
  let times = ref 0 in
  let vert_num = Imp.nb_vertex t in
  let start = Unix.gettimeofday () in
  List.iter (fun src -> List.iter (fun dst ->
    if not (src = dst) then let _ = generic_sp t src dst vert_num Imp.iter_succ in
                            times := !times +1
  ) hosts ) hosts;
  let stop = Unix.gettimeofday () in
  (List.length hosts, stop -. start,!times)

let _ =
  Arg.parse arg_spec (fun fn -> infname := fn) usage ;
  match !ttype with
    | Persistent ->
      Printf.printf "Running persistent version\n%!";
      let g = from_dotfile !infname in
      let n, t, t' = persistent_dijkstra g in
      Printf.printf "All-pairs shortest path between %d hosts took %f\n%!" n t;
      Printf.printf "Shortest path was called %d times\n%!" t'
    | Imperative ->
      Printf.printf "Running imperative version\n%!";
      let g = from_dotfile !infname in
      let g' = Imp.make_from_persistent g in
      let n, t, t' = imperative_dijkstra g' in
      Printf.printf "All-pairs shortest path between %d hosts took %f\n%!" n t;
      Printf.printf "Shortest path was called %d times\n%!" t'
    | CachedPersistent ->
      Printf.printf "Running persistent version with cached node hashes\n%!";
      let g = from_dotfile !infname in
      let g' = CachedPers.make_from_persistent g in
      let n, t, t' = cached_persistent_dijkstra g' in
      Printf.printf "All-pairs shortest path between %d hosts took %f\n%!" n t;
      Printf.printf "Shortest path was called %d times\n%!" t'
    | CachedImperative ->
      Printf.printf "Running imperative version with cached node hashes\n%!";
      let g = from_dotfile !infname in
      let g' = CachedImp.make_from_persistent g in
      let total, i, f, g = cached_imperative_dijkstra g' in
      Printf.printf "All-pairs shortest path %f\n%!" total;
      Printf.printf "Init:%f Find:%f Gather:%f\n%!" i f g
