# Apply

Apply operations to the current branch.

Operations for a given branch can be found via `gen --db db_name.db operations -b branch_name`. To apply an operation
from a given branch, use `gen --db db_name.db apply operation_id`. The changes will be applied to the current state of
the database and recorded as a new operation.

# Branches

Creating a new branch can be accomplished via `gen --db db_name.db branch --create branch_name`.
Deleting a branch can be accomplished via `gen --db db_name.db branch --delete branch_name`.
To list all available branches, `gen --db db_name.db branch --list`. The current branch will be marked with a `>` before it.
To checkout a branch, `gen --db db_name.db branch --checkout branch_name`. This will migrate the database to the last change
applied in a given branch.

# Operations

Operations are changes that have been made to the database. Commands such as `import` and `update` create a new operation.
To see all operations, `gen --db db_name.db operations` will list operations. The operation the database currently is on
will be prefixed with a `>`.

# Checkout

Checkouts allow a user to migrate the database to different states. To move the database to a given operation, the
command in `gen --db db_name.db checkout -b branch_name operation_id`. If no branch name is specified, the current
branch will be used. The operation_id corresponds to the operation in `operations`.