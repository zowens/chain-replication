#[macro_export]
macro_rules! rare {
    ($e:expr) => {
        unsafe { ::std::intrinsics::unlikely($e) }
    };
}
