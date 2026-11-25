from gadgetbridge_pipeline.defs.conformed.keys import stable_hash64


def test_hash_is_a_fixed_known_value_so_keys_never_change_across_builds_or_versions():
    # Golden value: if this changes, every stored session_key/fix_key is invalidated.
    assert stable_hash64("1@2000-01-01", "2000-01-01T00:00:00+00:00") == 5099394917115513953


def test_hash_is_a_signed_64_bit_int_that_depends_on_every_part_and_their_order():
    a = stable_hash64("x", "y")
    assert -(2**63) <= a < 2**63
    assert a != stable_hash64("y", "x")
    assert a != stable_hash64("x", "z")
    assert stable_hash64("ab", "c") != stable_hash64("a", "bc")  # no ambiguous concatenation
