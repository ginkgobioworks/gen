use std::hash::Hash;
use std::iter::from_fn;

use petgraph::visit::{IntoNeighborsDirected, NodeCount};
use petgraph::Direction;

// hacked from https://docs.rs/petgraph/latest/src/petgraph/algo/simple_paths.rs.html#36-102 to support digraphmap

pub fn all_simple_paths<G>(
    graph: G,
    from: G::NodeId,
    to: G::NodeId,
) -> impl Iterator<Item = Vec<G::NodeId>>
where
    G: NodeCount,
    G: IntoNeighborsDirected,
    G::NodeId: Eq + Hash,
{
    // list of visited nodes
    let mut visited = vec![from];
    // list of childs of currently exploring path nodes,
    // last elem is list of childs of last visited node
    let mut stack = vec![graph.neighbors_directed(from, Direction::Outgoing)];

    from_fn(move || {
        while let Some(children) = stack.last_mut() {
            if let Some(child) = children.next() {
                if child == to {
                    let path = visited.iter().cloned().chain(Some(to)).collect::<_>();
                    return Some(path);
                } else if !visited.contains(&child) {
                    visited.push(child);
                    stack.push(graph.neighbors_directed(child, Direction::Outgoing));
                }
            } else {
                stack.pop();
                visited.pop();
            }
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graphmap::DiGraphMap;
    use std::collections::HashSet;

    #[test]
    fn test_path_graph() {
        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);

        graph.add_edge(1, 2, ());
        graph.add_edge(2, 3, ());

        let paths = all_simple_paths(&graph, 1, 3).collect::<Vec<Vec<i32>>>();
        assert_eq!(paths.len(), 1);
        let path = paths.first().unwrap().clone();
        assert_eq!(path, vec![1, 2, 3]);
    }

    #[test]
    fn test_two_path_graph() {
        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);

        graph.add_edge(1, 2, ());
        graph.add_edge(1, 3, ());
        graph.add_edge(2, 4, ());
        graph.add_edge(3, 4, ());

        let paths = all_simple_paths(&graph, 1, 4).collect::<Vec<Vec<i32>>>();
        assert_eq!(paths.len(), 2);
        assert_eq!(
            HashSet::<Vec<i32>>::from_iter::<Vec<Vec<i32>>>(paths),
            HashSet::from_iter(vec![vec![1, 2, 4], vec![1, 3, 4]])
        );
    }

    #[test]
    fn test_two_by_two_combinatorial_graph() {
        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);

        graph.add_edge(1, 2, ());
        graph.add_edge(1, 3, ());
        graph.add_edge(2, 4, ());
        graph.add_edge(3, 4, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(4, 6, ());
        graph.add_edge(5, 7, ());
        graph.add_edge(6, 7, ());

        let paths = all_simple_paths(&graph, 1, 7).collect::<Vec<Vec<i32>>>();
        assert_eq!(paths.len(), 4);
        assert_eq!(
            HashSet::<Vec<i32>>::from_iter::<Vec<Vec<i32>>>(paths),
            HashSet::from_iter(vec![
                vec![1, 2, 4, 5, 7],
                vec![1, 3, 4, 5, 7],
                vec![1, 2, 4, 6, 7],
                vec![1, 3, 4, 6, 7]
            ])
        );
    }

    #[test]
    fn test_three_by_three_combinatorial_graph() {
        let mut graph: DiGraphMap<i32, ()> = DiGraphMap::new();
        graph.add_node(1);
        graph.add_node(2);
        graph.add_node(3);
        graph.add_node(4);
        graph.add_node(5);
        graph.add_node(6);
        graph.add_node(7);
        graph.add_node(8);
        graph.add_node(9);

        graph.add_edge(1, 2, ());
        graph.add_edge(1, 3, ());
        graph.add_edge(1, 4, ());
        graph.add_edge(2, 5, ());
        graph.add_edge(3, 5, ());
        graph.add_edge(4, 5, ());
        graph.add_edge(5, 6, ());
        graph.add_edge(5, 7, ());
        graph.add_edge(5, 8, ());
        graph.add_edge(6, 9, ());
        graph.add_edge(7, 9, ());
        graph.add_edge(8, 9, ());

        let paths = all_simple_paths(&graph, 1, 9).collect::<Vec<Vec<i32>>>();
        assert_eq!(paths.len(), 9);
        let expected_paths = vec![
            vec![1, 2, 5, 6, 9],
            vec![1, 3, 5, 6, 9],
            vec![1, 4, 5, 6, 9],
            vec![1, 2, 5, 7, 9],
            vec![1, 3, 5, 7, 9],
            vec![1, 4, 5, 7, 9],
            vec![1, 2, 5, 8, 9],
            vec![1, 3, 5, 8, 9],
            vec![1, 4, 5, 8, 9],
        ];
        assert_eq!(
            HashSet::<Vec<i32>>::from_iter::<Vec<Vec<i32>>>(paths),
            HashSet::from_iter(expected_paths)
        );
    }
}
