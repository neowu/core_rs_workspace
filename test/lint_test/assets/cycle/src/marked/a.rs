pub(super) fn item() {
    // lint: expect(module_cycle), reason
    super::b::item();
}
