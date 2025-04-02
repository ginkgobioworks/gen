#[cfg(test)]
mod standalone_sugiyama_tests {
    use crate::views::standalone_sugiyama::algorithm::{
        build_integer_layout_with_dummies, Edge, Vertex,
    };
    use crate::views::standalone_sugiyama::configure::{Config, CrossingMinimization, RankingType};
    use petgraph::stable_graph::StableDiGraph;

    fn make_ascii_grid(graph: StableDiGraph<Vertex, Edge>) -> Vec<Vec<char>> {
        // Calculate width and height from coordinates
        let (width, height) = graph.node_weights().fold((0, 0), |(max_x, max_y), vertex| {
            (max_x.max(vertex.x as usize), max_y.max(vertex.y as usize))
        });

        let mut grid = vec![vec![' '; width + 1]; height + 1];

        // Place nodes and dummy nodes
        for node in graph.node_indices() {
            let vertex = &graph[node];
            let x = vertex.x as usize;
            let y = vertex.y as usize;
            if vertex.is_dummy {
                grid[y][x] = '+';
            } else {
                grid[y][x] = (vertex.id as u8 + b'0') as char;
            }
        }

        grid
    }

    fn parse_ascii_grid(input: &str) -> (Vec<Vec<char>>, usize, usize) {
        let lines: Vec<&str> = input
            .lines()
            .map(|line| line.trim_end())
            .filter(|line| !line.is_empty())
            .collect();

        // Find the longest common whitespace prefix
        let common_prefix = lines
            .iter()
            .map(|line| line.chars().take_while(|c| c.is_whitespace()).count())
            .min()
            .unwrap_or(0);

        // Trim the common prefix from each line
        let lines: Vec<&str> = lines.iter().map(|line| &line[common_prefix..]).collect();

        let height = lines.len();
        let width = lines.iter().map(|line| line.len()).max().unwrap_or(0);

        let mut grid = vec![vec![' '; width]; height];

        for (y, line) in lines.iter().enumerate() {
            for (x, c) in line.chars().enumerate() {
                grid[y][x] = c;
            }
        }

        (grid, width, height)
    }

    #[test]
    fn test_sugiyama() {
        let edges = [
            (0, 1),
            (0, 2),
            (1, 3),
            (1, 4),
            (1, 5),
            (1, 6),
            (3, 7),
            (3, 8),
            (4, 7),
            (4, 8),
            (5, 7),
            (5, 8),
            (6, 7),
            (6, 8),
            (7, 9),
            (8, 9),
            (2, 9),
        ];

        let mut graph = StableDiGraph::new();
        let mut vertices = Vec::new();

        for i in 0..10 {
            let v = graph.add_node(Vertex::new(i, (1.0, 1.0)));
            vertices.push(v);
        }

        for (from, to) in edges {
            graph.add_edge(vertices[from], vertices[to], Edge::default());
        }

        let config = Config {
            minimum_length: 1,
            ranking_type: RankingType::MinimizeEdgeLength,
            c_minimization: CrossingMinimization::Barycenter,
            transpose: true,
            dummy_vertices: true,
            dummy_size: 1.0,
            ..Default::default()
        };

        // Build the layout
        let graph = build_integer_layout_with_dummies(graph.clone(), &config);
        let coords: Vec<(usize, (i32, i32))> = graph.node_weights().map(|v| (v.id, (v.x, v.y))).collect();
        
        assert_eq!(coords.len(), 12); // 10 nodes + 2 dummy nodes

        // Print a simple ASCII visualization
        println!("\nASCII Visualization (1x1 nodes with spacing of 1):");
        let ascii_grid = make_ascii_grid(graph);
        for row in ascii_grid.clone() {
            println!("  {}", row.iter().collect::<String>());
        }

        // Confirm that the ascii grid is correct
        let (ref_grid, ref_width, ref_height) = parse_ascii_grid(
            "
              0    
          2    1   
          + 6 5 4 3
          +   8 7  
              9 
        ",
        );

        assert_eq!(ascii_grid, ref_grid);

    }


    #[test]
    fn test_deletion() {
        let edges = [
            (0 ,1),
            (1, 2),
            (1, 3),
            (2, 3),
            (3, 4),
        ];

        let mut graph = StableDiGraph::new();
        let mut vertices = Vec::new();

        for i in 0..5 {
            let v = graph.add_node(Vertex::new(i, (1.0, 1.0)));
            vertices.push(v);
        }

        for (from, to) in edges {
            graph.add_edge(vertices[from], vertices[to], Edge::default());
        }

        let config = Config {
            minimum_length: 1,
            ranking_type: RankingType::MinimizeEdgeLength,
            c_minimization: CrossingMinimization::Barycenter,
            transpose: true,
            dummy_vertices: true,
            dummy_size: 0.0,
            ..Default::default()
        };

        // Build the layout
        let graph = build_integer_layout_with_dummies(graph.clone(), &config);
        let coords: Vec<(usize, (i32, i32))> = graph.node_weights().map(|v| (v.id, (v.x, v.y))).collect();
        
        //assert_eq!(coords.len(), 12); // 10 nodes + 2 dummy nodes

        // Print a simple ASCII visualization
        println!("\nASCII Visualization (1x1 nodes with spacing of 1):");
        let ascii_grid = make_ascii_grid(graph);
        for row in ascii_grid.clone() {
            println!("  {}", row.iter().collect::<String>());
        }

        // Confirm that the ascii grid is correct
        let (ref_grid, ref_width, ref_height) = parse_ascii_grid(
            "
              0    
          2    1   
          + 6 5 4 3
          +   8 7  
              9 
        ",
        );

        assert_eq!(ascii_grid, ref_grid);

    }
}
