# Greaters
This creates a library that contains an implementation of function `greatest` from PySpark.


## Docs
This is a test task, so you can up rust docs yourself.

Git clone the project and run
```
cargo doc --open
```
This will open documentation details in your browser

## Implementation difficulties

The main difficulties come from different approaches to API design. 
Datafuse has a rust-like API with a very strict type policy.
PySpark on the other side doesn't restrict types as much as possible.

PySpark works with all types as long as they are the same, to reproduce this
behavior in Rust we need to abstract over particular types like `Float64` or `Uint32` 
to the general type. For this purpose in datafuse, there is `ScalarValue` but you can't 
simply build an array out of these. For optimization purposes, builders API differs for primitive
and not primitive builders. That's why the `append_value` method is not in the general `ArrayBuilder`
trait.

### Solution
I extended `ArrayBuilder` with the trait `AppendableBuilder` and implemented it for all necessary builder traits
from the database library (also used macros for simple cases). Then abstract logic over the concrete implementation
of `AppendableBuilder` and write the logic for `greatest`. For code readability, I created a variation of `create_builder`
from the standard library, but for `AppendableBuilder` to consize the function with actual logic.

### Alternative solutions
The alternative solution is to use rust another mechanism of polymorphism: enums.
Make builder enum and implement for it builder trait and pattern match on scalar value.

This will result in a valid, but less decomposable solution because it will be
restricted by enum variants, my solution can be extended to types not defined 
in the database with less effort.