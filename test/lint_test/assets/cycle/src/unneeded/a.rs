pub(super) fn item() {
    // lint: expect(module_cycle)
    super::b::item();
}
