pub(super) fn run() {
    let _shown = crate::a::four();
    crate::a::ten();
    // also called by a test of another crate, which could not reach it once private
    crate::a::eleven();
    let _local = crate::parts::Local;
}
