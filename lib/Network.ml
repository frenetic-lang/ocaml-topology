module type VERTEX = sig
  type t

  val compare : t -> t -> int
  val to_string : t -> string
  val to_dot : t -> string
  val parse_dot : Graph.Dot_ast.node_id -> Graph.Dot_ast.attr list -> t
  val parse_gml : Graph.Gml.value_list -> t
end

module type EDGE = sig
  type t

  val compare : t -> t -> int
  val to_string : t -> string
  val to_dot : t -> string
  val parse_dot : Graph.Dot_ast.attr list -> t
  val parse_gml : Graph.Gml.value_list -> t
  val default : t
end

module type WEIGHT = sig
  type t
  type label

  val weight : label -> t
  val compare : t -> t -> int
  val add : t -> t -> t
  val zero : t
end

module type NETWORK = sig
  module Topology : sig
    type t

    type vertex
    type edge
    type port = int32

    module Vertex : VERTEX
    module Edge : EDGE

    module UnitWeight : WEIGHT 
      with type t = int 
      and type label = Edge.t

    module EdgeSet : Set.S
      with type elt = edge

    module VertexSet : Set.S
      with type elt = vertex

    module VertexHash : Hashtbl.S
      with type key = vertex

    module PortSet : Set.S
      with type elt = port

    (* Constructors *)
    val copy : t -> t
    val empty : unit -> t
    val add_vertex : t -> Vertex.t -> (t * vertex)
    val add_edge : t -> vertex -> port -> Edge.t -> vertex -> port -> (t * edge)

    (* Special Accessors *)
    val num_vertexes : t -> int
    val num_edges : t -> int
    val vertexes : t -> VertexSet.t
    val edges : t -> EdgeSet.t
    val neighbors : t -> vertex -> VertexSet.t
    val find_edge : t -> vertex -> vertex -> edge
    val vertex_to_ports : t -> vertex -> PortSet.t
    val next_hop : t -> vertex -> port -> edge option
    val edge_src : edge -> (vertex * port)
    val edge_dst : edge -> (vertex * port)
    val inverse_edge : t -> edge -> edge option

    (* Label Accessors *)
    val vertex_to_string : t -> vertex -> string
    val vertex_to_label : t -> vertex -> Vertex.t
    val vertex_of_label : t -> Vertex.t -> vertex
    val edge_to_string : t -> edge -> string
    val edge_to_label : t -> edge -> Edge.t

    (* Iterators *)
    val iter_succ : (edge -> unit) -> t -> vertex -> unit
    val iter_vertexes : (vertex -> unit) -> t -> unit
    val iter_edges : (edge -> unit) -> t -> unit
    val fold_vertexes : (vertex -> 'a -> 'a) -> t -> 'a -> 'a
    val fold_edges : (edge -> 'a -> 'a) -> t -> 'a -> 'a

    (* Mutators *)
    val remove_vertex : t -> vertex -> t
    val remove_edge : t -> edge -> t
    val remove_endpoint : t -> (vertex * port) -> t
  end

  (* Traversals *)
  module Traverse : sig
    val bfs : (Topology.vertex -> unit) -> Topology.t -> unit
    val dfs : (Topology.vertex -> unit) -> Topology.t -> unit
  end

  (* Paths *)
  module type PATH = sig 
    type weight 
    type t = Topology.edge list
    exception NegativeCycle of t
        
    val shortest_path : Topology.t -> Topology.vertex -> Topology.vertex -> t option
    val all_shortest_paths : Topology.t -> Topology.vertex -> Topology.vertex Topology.VertexHash.t
    val all_pairs_shortest_paths : Topology.t
      -> (weight * Topology.vertex * Topology.vertex * Topology.vertex list) list
  end

  module Path (Weight : WEIGHT with type label = Topology.Edge.t) : 
    PATH with type weight = Weight.t

  module UnitPath : PATH
    with type weight = int

  (* Parsing *)
  module Parse : sig
    val from_dotfile : string -> Topology.t
    val from_gmlfile : string -> Topology.t
  end

  (* Pretty Printing *)
  module Pretty : sig
    val to_string : Topology.t -> string
    val to_dot : Topology.t -> string
  end
end

module type MAKE = functor (Vertex:VERTEX) -> functor (Edge:EDGE) -> NETWORK
  with module Topology.Vertex = Vertex
   and module Topology.Edge = Edge

module Make : MAKE =
  functor (Vertex:VERTEX) ->
    functor (Edge:EDGE) ->
struct
  module Topology = struct
    type port = int32
    module PortSet = Set.Make(Int32)
    module PortMap = Map.Make(Int32)

    module Vertex = Vertex
    module Edge = Edge

    module VL = struct
      type t =
          { id : int;
            label : Vertex.t }
      let compare n1 n2 = Pervasives.compare n1.id n2.id
      let hash n1 = Hashtbl.hash n1.id
      let equal n1 n2 = n1.id = n2.id
      let to_string n = string_of_int n.id
    end
    module VertexSet = Set.Make(VL)
    module VertexMap = Map.Make(Vertex)
    module VertexHash = Hashtbl.Make(VL)

    module EL = struct
      type t = { id : int;
                 label : Edge.t;
                 src : port;
                 dst : port }
      let compare e1 e2 = Pervasives.compare e1.id e2.id
      let hash e1 = Hashtbl.hash e1.id
      let equal e1 e2 = e1.id = e2.id
      let to_string e = string_of_int e.id
      let default =
        { id = 0;
          label = Edge.default;
          src = 0l;
          dst = 0l }
    end

    module UnitWeight = struct
      type label = Edge.t
      type t = int
      let weight _ = 1
      let compare = Pervasives.compare
      let add = (+)
      let zero = 0
    end

    module EdgeSet = Set.Make(struct
      type t = VL.t * EL.t * VL.t
      let compare (e1:t) (e2:t) : int =
        let (_,l1,_) = e1 in
        let (_,l2,_) = e2 in
        EL.compare l1 l2
    end)

    module P = Graph.Persistent.Digraph.ConcreteBidirectionalLabeled(VL)(EL)
    module EdgeMap = Map.Make (EL)

    type vertex = P.vertex

    type edge = P.edge

    type t =
        { graph : P.t;
          node_labels : vertex VertexMap.t;
          inverse_map : edge EdgeMap.t;
          next_node : int;
          next_edge : int }

    (* Constructors *)
    let copy (t:t) : t =
      t

    let empty () : t =
      { graph = P.empty;
        node_labels = VertexMap.empty;
        inverse_map = EdgeMap.empty;
        next_node = 0;
        next_edge = 0 }

    let add_inverse_map (t : t) (e1 : edge) (e2 : edge) =
      let _,e1t,_ = e1 in
      let _,e2t,_ = e2 in
      let oldmap = t.inverse_map in
      let map1 = EdgeMap.add e1t e2 oldmap in
      let map2 = EdgeMap.add e2t e1 map1 in
      { t with inverse_map = map2 }

    let inverse_edge (t : t) (e : edge) =
      let _,e_t,_ = e in
      try Some (EdgeMap.find e_t t.inverse_map)
      with Not_found -> None

    let add_vertex (t:t) (l:Vertex.t) : t * vertex =
      let open VL in
      try (t, VertexMap.find l t.node_labels)
      with Not_found ->
        let id = t.next_node + 1 in
        let v = { id = id; label = l } in
        let g = P.add_vertex t.graph v in
        let nl = VertexMap.add l v t.node_labels in
        ({ t with graph=g; node_labels=nl; next_node = id}, v)

    let add_edge (t:t) (v1:vertex) (p1:port) (l:Edge.t) (v2:vertex) (p2:port) : t * edge =
      let open EL in
      let aux t =
        let id = t.next_edge + 1 in
        let l = { id = id; label = l; src = p1; dst = p2 } in
        let e = (v1,l,v2) in
        ({ t with graph = P.add_edge_e t.graph e; next_edge = id }, e) in      
      try
        let es = P.find_all_edges t.graph v1 v2 in
        let es' = List.filter ( fun (s,l,d) ->
          l.src = p1 && l.dst = p2 ) es in
        match es' with
          | [] -> aux t
          | es ->
            let graph' = List.fold_left (fun acc e ->
            P.remove_edge_e acc e) t.graph es in
            let t' = {t with graph = graph'} in
            aux t'
      with Not_found -> aux t

    (* Special Accessors *)
    let num_vertexes (t:t) : int =
      P.nb_vertex t.graph

    let num_edges (t:t) : int =
      P.nb_edges t.graph

    let edges (t:t) : EdgeSet.t =
      P.fold_edges_e EdgeSet.add t.graph EdgeSet.empty

    let vertexes (t:t) : VertexSet.t =
      P.fold_vertex VertexSet.add t.graph VertexSet.empty

    let neighbors (t:t) (v:vertex) : VertexSet.t =
      P.fold_succ VertexSet.add t.graph v VertexSet.empty

    let find_edge (t:t) (src:vertex) (dst:vertex) : edge =
      P.find_edge t.graph src dst

    let vertex_to_string (t:t) (v:vertex) : string =
      VL.to_string v

    let vertex_to_label (t:t) (v:vertex) : Vertex.t =
      v.VL.label

    let vertex_of_label (t:t) (l:Vertex.t) : vertex =
      VertexMap.find l t.node_labels

    let edge_to_label (t:t) (e:edge) : Edge.t =
      let (_,l,_) = e in
      l.EL.label

    let edge_to_string (t:t) (e:edge) : string =
      let (_,e,_) = e in 
      EL.to_string e

    let edge_src (e:edge) : (vertex * port) =
      let (v1,l,_) = e in
      (v1, l.EL.src)

    let edge_dst (e:edge) : (vertex * port) =
      let (_,l,v2) = e in
      (v2, l.EL.dst)

    let next_hop (t:t) (v1:vertex) (p:port) : edge option =
      let rec loop es = match es with
        | [] -> None
        | ((_,l,v2) as e)::es' ->
          if l.EL.src = p
            then Some e
            else (loop es') in
      loop (P.succ_e t.graph v1)

    let vertex_to_ports (t:t) (v1:vertex) : PortSet.t =
      List.fold_left
        (fun a e ->
          let _,l,_ = e in
          PortSet.add l.EL.src a)
        PortSet.empty
        (P.succ_e t.graph v1)

    (* Iterators *)
    let fold_vertexes (f:vertex -> 'a -> 'a) (t:t) (init:'a) : 'a =
      P.fold_vertex f t.graph init

    let fold_edges (f:edge -> 'a -> 'a) (t:t) (init:'a) : 'a =
      P.fold_edges_e f t.graph init

    let iter_vertexes (f:vertex -> unit) (t:t) : unit =
      P.iter_vertex f t.graph

    let iter_edges (f:edge -> unit) (t:t) : unit =
      P.iter_edges_e f t.graph

    let iter_succ (f:edge -> unit) (t:t) (v:vertex) : unit =
      P.iter_succ_e f t.graph v

    (* Mutators *)
    let remove_vertex (t:t) (v:vertex) : t =
      let graph = P.remove_vertex t.graph v in
      let node_labels = VertexMap.remove v.VL.label t.node_labels in
      { t with graph; node_labels }

    let remove_edge (t:t) (e:edge) : t =
      { t with graph = P.remove_edge_e t.graph e }

    let remove_endpoint (t:t) (ep : vertex * port) : t =
      fold_edges (fun e acc ->
        if edge_src e = ep || edge_dst e = ep
          then remove_edge acc e
          else acc)
        t t
  end

  (* Traversals *)
  module Traverse = struct
    open Topology
    module Bfs = Graph.Traverse.Bfs(P)
    module Dfs = Graph.Traverse.Dfs(P)

    let bfs (f:vertex -> unit) (t:t) =
      Bfs.iter f t.graph

    let dfs (f:vertex -> unit) (t:t) =
      Dfs.prefix f t.graph
  end

  (* Paths *)
  module type PATH = sig 
    type weight 
    type t = Topology.edge list
    exception NegativeCycle of t
        
    val shortest_path : Topology.t -> Topology.vertex -> Topology.vertex -> t option
    val all_shortest_paths : Topology.t -> Topology.vertex -> Topology.vertex Topology.VertexHash.t
    val all_pairs_shortest_paths : Topology.t
      -> (weight * Topology.vertex * Topology.vertex * Topology.vertex list) list
  end

  module Path = functor (Weight : WEIGHT with type label = Topology.Edge.t) ->
  struct
    open Topology

    module WL = struct
      type t = Weight.t
      type label = EL.t

      let weight l = Weight.weight (l.EL.label)
      let compare = Weight.compare
      let add = Weight.add
      let zero = Weight.zero
    end 

    module Dijkstra = Graph.Path.Dijkstra(P)(WL)

    type weight = Weight.t
    type t = edge list

    let shortest_path (t:Topology.t) (v1:vertex) (v2:vertex) : t option =
      try
        let pth,_ = Dijkstra.shortest_path t.graph v1 v2 in
        Some pth
      with Not_found ->
        None

    exception NegativeCycle of edge list
    (* Implementation of Bellman-Ford algorithm, based on that in ocamlgraph's
       Path library. Returns a hashtable mapping each node to its predecessor in
       the path *)
    let all_shortest_paths (t:Topology.t) (src:vertex) : (vertex VertexHash.t) =
      let size = P.nb_vertex t.graph in
      let dist = VertexHash.create size in
      let prev = VertexHash.create size in
      let admissible = VertexHash.create size in
      VertexHash.replace dist src Weight.zero;
      let build_cycle_from x0 =
        let rec traverse_parent x ret =
          let e = VertexHash.find admissible x in
          let s,_ = edge_src e in
          if s = x0 then e :: ret else traverse_parent s (e :: ret) in 
        traverse_parent x0 [] in 
      let find_cycle x0 =
        let rec visit x visited =
          if VertexSet.mem x visited then
            build_cycle_from x
          else begin
            let e = VertexHash.find admissible x in
            let s,_ = edge_src e in
            visit s (VertexSet.add x visited)
          end
        in
        visit x0 (VertexSet.empty)
      in
      let rec relax (i:int) =
        let update = P.fold_edges_e
          (fun e x ->
            let ev1,_ = edge_src e in
            let ev2,_ = edge_dst e in
            try begin
              let dev1 = VertexHash.find dist ev1 in
              let dev2 = Weight.add dev1 (Weight.weight (Topology.edge_to_label t e)) in
              let improvement =
                try Weight.compare dev2 (VertexHash.find dist ev2) < 0
                with Not_found -> true
              in
              if improvement then begin
                VertexHash.replace prev ev2 ev1;
                VertexHash.replace dist ev2 dev2;
                VertexHash.replace admissible ev2 e;
                Some ev2
              end else x
            end with Not_found -> x) t.graph None in
        match update with
          | Some x ->
            if i == P.nb_vertex t.graph then raise (NegativeCycle (find_cycle x))
            else relax (i + 1)
          | None -> prev in 
      let r = relax 0 in
      r

    let all_pairs_shortest_paths (t:Topology.t) : (Weight.t * vertex * vertex * vertex list) list =
      (* Because Weight does not provide infinity, we lift Weight.t
         using an option: None corresponds to infinity, and Some w
         corresponds to a finite weight. *)
      let add_opt o1 o2 =
        match o1, o2 with
          | Some w1, Some w2 -> Some (Weight.add w1 w2)
          | _ -> None in
      let lt_opt o1 o2 =
        match o1, o2 with
          | Some w1, Some w2 -> Weight.compare w1 w2 < 0
          | Some _, None -> true
          | None, Some _ -> false
          | None, None -> false in
      let make_matrix (g:Topology.t) =
        let n = P.nb_vertex g.graph in
        let vs = vertexes g in
        let nodes = Array.make n (VertexSet.choose vs) in
        let _ = VertexSet.fold (fun v i -> Array.set nodes i v; i+1) vs 0 in
        (Array.init n
           (fun i -> Array.init n
             (fun j -> if i = j then (Some Weight.zero, [nodes.(i)])
               else
                 try
                   let l = find_edge g nodes.(i) nodes.(j) in
                   let w = Weight.weight (Topology.edge_to_label g l) in
                   (Some w, [nodes.(i); nodes.(j)])
                 with Not_found -> (None,[]))),
         nodes)
      in
      let matrix,vxs = make_matrix t in
      let n = P.nb_vertex t.graph in
      let dist i j = fst (matrix.(i).(j)) in
      let path i j = snd (matrix.(i).(j)) in
      for k = 0 to n - 1 do
        for i = 0 to n - 1 do
          for j = 0 to n - 1 do
            let dist_ikj = add_opt (dist i k) (dist k j) in
            if lt_opt dist_ikj (dist i j) then
              matrix.(i).(j) <- (dist_ikj, path i k @ List.tl (path k j))
          done
        done
      done;
      let paths = ref [] in
      Array.iteri (fun i array ->
        Array.iteri (fun j elt ->
          match elt with 
            | None, _ -> ()
            | Some w, p -> 
              paths := (w, vxs.(i), vxs.(j),p) :: !paths) 
          array;) 
        matrix;
      !paths
  end

  module UnitPath = Path(Topology.UnitWeight)

  (* Parsing *)
  module Parse = struct
    open Topology
    (* TODO(jnf): this could be refactored into a functor that wraps a
       G.t in an arbitrary type and lifts all other G operations over
       that type. *)
    module Build = struct
      module G = struct
        module V = P.V
        module E = P.E
        type vertex = V.t
        type edge = E.t
        type t = Topology.t
        let empty () =
          empty ()
        let remove_vertex t v =
          { t with graph = P.remove_vertex t.graph v }
        let remove_edge t v1 v2 =
          { t with graph = P.remove_edge t.graph v1 v2 }
        let remove_edge_e t e =
          { t with graph = P.remove_edge_e t.graph e }
        let add_vertex t v =
          { t with graph = P.add_vertex t.graph v ;
            node_labels = VertexMap.add v.Topology.VL.label v t.node_labels;
            next_node = v.Topology.VL.id + 1}
        let add_edge t v1 v2 =
          let new_t = { t with graph = P.add_edge t.graph v1 v2 ; next_edge = t.next_edge + 1} in
          try
            let open Topology.EL in
            let (_,r,_) as reverse = P.find_edge t.graph v2 v1 in
            let (_,te,_) as this_edge = P.find_edge new_t.graph v1 v2 in
            if r.src = te.dst && r.dst = te.src then
              Topology.add_inverse_map new_t this_edge reverse
            else new_t
          with Not_found -> new_t
        let add_edge_e t e =
          let open Topology.EL in
          let (v1,l,v2) = e in
          let new_t = { t with graph = P.add_edge_e t.graph e ;
            next_edge = l.Topology.EL.id + 1} in
          try
            let (_,r,_) as reverse = P.find_edge t.graph v2 v1 in
            if r.src = l.dst && r.dst = l.src then
              Topology.add_inverse_map new_t e reverse
            else new_t
          with Not_found -> new_t
        let fold_pred_e f t i =
          P.fold_pred_e f t.graph i
        let iter_pred_e f t =
          P.iter_pred_e f t.graph
        let fold_succ_e f t i =
          P.fold_succ_e f t.graph i
        let iter_succ f t v =
          P.iter_succ f t.graph v
        let iter_succ_e f t v =
          P.iter_succ_e f t.graph v
        let iter_edges f t =
          P.iter_edges f t.graph
        let fold_pred f t v i =
          P.fold_pred f t.graph v i
        let fold_succ f t v i =
          P.fold_succ f t.graph v i
        let iter_pred f t v =
          P.iter_pred f t.graph v
        let map_vertex f t =
          { t with graph = P.map_vertex f t.graph }
        let fold_edges_e f t i =
          P.fold_edges_e f t.graph i
        let iter_edges_e f t =
          P.iter_edges_e f t.graph
        let fold_vertex f t i =
          P.fold_vertex f t.graph i
        let fold_edges f t i =
          P.fold_edges f t.graph i
        let iter_vertex f t =
          P.iter_vertex f t.graph
        let pred_e t v  =
          P.pred_e t.graph v
        let succ_e t v =
          P.succ_e t.graph v
        let pred t v =
          P.pred t.graph v
        let succ t v =
          P.succ t.graph v
        let find_all_edges t v1 v2 =
          P.find_all_edges t.graph v1 v2
        let find_edge t v1 v2 =
          P.find_edge t.graph v1 v2
        let mem_edge_e t e =
          P.mem_edge_e t.graph e
        let mem_edge t v1 v2 =
          P.mem_edge t.graph v1 v2
        let mem_vertex t v =
          P.mem_vertex t.graph v
        let in_degree t v =
          P.in_degree t.graph v
        let out_degree t v =
          P.out_degree t.graph v
        let nb_edges t =
          P.nb_edges t.graph
        let nb_vertex t =
          P.nb_vertex t.graph
        let is_empty t =
          P.is_empty t.graph
        let is_directed =
          P.is_directed
      end
      let empty = G.empty
      let remove_vertex = G.remove_vertex
      let remove_edge = G.remove_edge
      let remove_edge_e = G.remove_edge_e
      let add_vertex = G.add_vertex
      let add_edge = G.add_edge
      let add_edge_e = G.add_edge_e
      let copy t = t
    end
    module Dot = Graph.Dot.Parse(Build)(struct
      let get_port o = match o with
        | Some(s) -> begin match s with
            | Graph.Dot_ast.Number(i) -> Scanf.sscanf i "%lu" (fun i -> i)
            | _ -> failwith "Requires number" end
        | None -> failwith "Requires value"
      let next_node = let r = ref 0 in fun _ -> incr r; !r
      let next_edge = let r = ref 0 in fun _ -> incr r; !r
      let node id attrs =
        let open VL in
            { id = next_node ();
              label = Vertex.parse_dot id attrs }
      let edge attrs =
        (* This is a bit of a hack because we only look at the first list of attrs *)
        let ats = List.hd attrs in
        let src,dst,rest = List.fold_left (fun (src,dst,acc) (k,v) -> match k with
          | Graph.Dot_ast.Ident("src_port") -> (get_port v,dst,acc)
          | Graph.Dot_ast.Ident("dst_port") -> (src, get_port v, acc)
          | _ -> (src,dst,(k,v)::acc)
        ) (0l,0l,[]) ats in
        let attrs' = rest::(List.tl attrs) in
        let open EL in
            { id = next_edge ();
              label = Edge.parse_dot attrs';
              src = src;
              dst = dst }
    end)
    module Gml = Graph.Gml.Parse(Build)(struct
      let next_node = let r = ref 0 in fun _ -> incr r; !r
      let next_edge = let r = ref 0 in fun _ -> incr r; !r
      let node vs =
        let open VL in
            { id = next_node ();
              label = Vertex.parse_gml vs }
      let edge vs =
        let open EL in
            { id = next_edge ();
              label = Edge.parse_gml vs;
              src = 0l;
              dst = 0l }
    end)

    let from_dotfile = Dot.parse
    let from_gmlfile = Gml.parse
  end

  (* Pretty Printing *)
  module Pretty = struct
    open Topology
    let to_dot (t:t) =
      let es = (EdgeSet.fold (fun (s,l,d) acc ->
        let _,src_port = edge_src (s,l,d) in
        let _,dst_port = edge_dst (s,l,d) in
        Printf.sprintf "%s%s%s -> %s {src_port=%lu; dst_port=%lu; %s};"
          acc
          (if acc = "" then "" else "\n")
          (Vertex.to_string s.VL.label)
          (Vertex.to_string d.VL.label)
          src_port
          dst_port
          (Edge.to_dot l.EL.label))
                  (edges t) "") in
      let vs = (VertexSet.fold (fun v acc ->
        Printf.sprintf "%s%s\n%s;"
          acc
          (if acc = "" then "" else "\n")
          (Vertex.to_dot v.VL.label)
      ) (vertexes t) "") in
      Printf.sprintf "digraph G {\n%s\n%s\n}\n" vs es

    let to_string (t:t) : string =
      to_dot t
  end
end

