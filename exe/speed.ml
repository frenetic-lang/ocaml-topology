open Graph
open Sexplib.Conv
open Topology

type topoType =
  | Persistent
  | Imperative

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

module Pers = struct
  include Topology
  let shortest_path t src dst =
    let visited = Hashtbl.create (nb_vertex t) in
    let previous =  Hashtbl.create (nb_vertex t) in
    let queue = Core.Std.Heap.create (fun (v,d) (v,d') -> compare d d') () in
    Core.Std.Heap.add queue (src,0);

    let rec mk_path current =
      if current = src then [src] else
        let prev = Hashtbl.find previous current in
        prev::(mk_path prev) in

    let rec loop (current,distance) =
      if current = dst then ()
      else begin
        iter_succ (fun next ->
          if Hashtbl.mem visited next then ()
          else
            let next_dist = distance + 1 in
            Hashtbl.replace previous next current;
            Core.Std.Heap.add queue (next,next_dist)
        ) t current;
        Hashtbl.replace visited current 0;
        loop (Core.Std.Heap.pop_exn queue) end in
    loop (Core.Std.Heap.pop_exn queue);
    mk_path dst

end

module Imp = struct
  module G = Imperative.Digraph.ConcreteBidirectionalLabeled(Node)(Link)
  include G
  module Dij = Path.Dijkstra(G)(Weight)

  let make_from_persistent p =
    let g = create ~size:(Topology.nb_edges p) () in
    Topology.iter_edges_e (add_edge_e g) p;
    g

  let shortest_path t src dst =
    let visited = Hashtbl.create (nb_vertex t) in
    let previous =  Hashtbl.create (nb_vertex t) in
    let queue = Core.Std.Heap.create (fun (v,d) (v,d') -> compare d d') () in
    Core.Std.Heap.add queue (src,0);

    let rec mk_path current =
      if current = src then [src] else
        let prev = Hashtbl.find previous current in
        prev::(mk_path prev) in

    let rec loop (current,distance) =
      if current = dst then ()
      else begin
        iter_succ (fun next ->
          if Hashtbl.mem visited next then ()
          else
            let next_dist = distance + 1 in
            Hashtbl.replace previous next current;
            Core.Std.Heap.add queue (next,next_dist)
        ) t current;
        Hashtbl.replace visited current 0;
        loop (Core.Std.Heap.pop_exn queue) end in
    loop (Core.Std.Heap.pop_exn queue);
    mk_path dst
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
    ; ("-o",
       Arg.String (fun s -> outfname := s ),
       "\tWrite topology to a file")
]

let usage = Printf.sprintf "usage: %s [--dot|--gml] filename -o filename [--dot|--mn]" Sys.argv.(0)

let persistent_dijkstra (t:Pers.t) : (int * float) =
  let hosts = Pers.get_hosts t in
  let start = Unix.gettimeofday () in
  List.iter (fun src -> List.iter (fun dst ->
    if not (src = dst) then let _ = Pers.shortest_path t src dst in ()
  ) hosts ) hosts;
  let stop = Unix.gettimeofday () in
  (List.length hosts, stop -. start)

let imperative_dijkstra (t:Imp.t) : (int * float) =
  let hosts = Imp.fold_vertex (fun v acc -> match v with
      | Node.Host(_) -> v::acc
      | _ -> acc
    ) t [] in
  let start = Unix.gettimeofday () in
  List.iter (fun src -> List.iter (fun dst ->
    if not (src = dst) then let _ = Imp.shortest_path t src dst in ()
  ) hosts ) hosts;
  let stop = Unix.gettimeofday () in
  (List.length hosts, stop -. start)

let _ =
  Arg.parse arg_spec (fun fn -> infname := fn) usage ;
  match !ttype with
    | Persistent ->
      Printf.printf "Running persistent version\n%!";
      let g = from_dotfile !infname in
      let n, t = persistent_dijkstra g in
      Printf.printf "All-pairs shortest path between %d hosts took %f\n%!" n t
    | Imperative ->
      Printf.printf "Running imperative version\n%!";
      let g = from_dotfile !infname in
      let g' = Imp.make_from_persistent g in
      let n, t = imperative_dijkstra g' in
      Printf.printf "All-pairs shortest path between %d hosts took %f\n%!" n t

