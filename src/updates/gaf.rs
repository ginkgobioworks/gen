pub fn update_with_gaf() {}

mod tests {
    use super::*;
    use crate::test_helpers::{get_connection, get_operation_connection, setup_block_group};

    #[test]
    fn test_x() {
        let conn = &get_connection(None);
        let op_conn = &get_operation_connection(None);
        let (bg_1, path) = setup_block_group(conn);
    }
}
