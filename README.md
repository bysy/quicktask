# quicktask
Quicktask is a C library for parallel task splitting and executing. I wanted
a library that didn't rely on a central scheduler or pre-split tasks: The
resulting design uses a concurrent tree structure to hold the tasks; the
worker threads traverse the tree by following rules that guarantee thread
separation and cache locality.

The tree structure uses spin locks with a simple locking strategy. Along with the traversal rules, this helps ensure low contention as well.

## Maturity
It's proof of concept. The design started on a piece of paper and together with
the implementation of the core functionality took about 5 days. There's still
some polishing and rewriting left to do. A wishlist project I want to carry out
is rewriting the code in such a way that correctness can be demonstrated up to
a certain combinatorial depth (preempting patterns). Last, I'd love to have
support for parallel continuations; it's needed for integration with
[VH](https://github.com/bysy/VanHaskell).
