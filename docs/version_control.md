# Version Control

Gen enables version control similar to git with the sequence database. Many version control like commands in gen are
direct analogs of git commands. Slight differences are:

* Commits are called operations. This is because there is not the same kind of straightforward editing of text files 
that is common in git in biological engineering. Thus, many things we do to our sequences are operations -- such as
inserting a gene in a position.
* There is no concept of a staging area for changes. Whereas git will wait for the user to package together work 
into commits, operations are applied right away. 

A summary of commands implemented in Gen and their counterparts in git are:

| Command             | Gen                             | Git                       |
|---------------------|---------------------------------|---------------------------|
| Create a new branch | gen branch --create new_branch  | git branch new_branch     | 
| Delete a branch     | gen branch --delete branch_name | git branch -D branch_name |
| List all branches   | gen branch --list               | git branch                |
| Merge a branch      | gen branch --merge branch_name  | git merge branch_name     |
| Checkout a branch   | gen checkout -b branch_name     | git checkout branch_name  |
| Checkout a commit   | gen checkout <operation_id>     | git checkout commit       |
| Reset to a commit   | gen reset <operation_id>        | git reset commit          |
| List work history   | gen operations <branch_name>    | git log                   |
| Apply an operation  | gen apply <operation_id>        | git apply commit_hash     |

  