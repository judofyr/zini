# Zini

Zini (Zig + Mini) is a [Zig](https://ziglang.org/) library providing some succinct data structures.

The main contribution is `zini.pthash` which is an implementation of [PTHash][pthash], a [minimal perfect hash function](https://en.wikipedia.org/wiki/Perfect_hash_function) construction algorithm.
Given a set of `n` elements, with the only requirement being that you can hash them, it generates a hash function which maps each element to a distinct number between `0` and `n - 1`.
The generated hash function is extremely small, typically consuming less than **5 _bits_ per element**, regardless of the size of the input type.
The algorithm provides multiple parameters to tune making it possible to optimize for (small) size, (short) construction time, or (short) lookup time.

In addition, Zini provides structs for storing arrays of **64-bits numbers in a compact manner**:

- `zini.CompactArray` stores n-bit numbers tightly packed, leaving no bits unused.
  If the largest value in an array is `m` then you actually only need `n = log2(m) + 1` bits per element.
  E.g. if the largest value is 270, you will get 7x compression using CompactArray over `[]u64` as it stores each element using only 9 bits (and 64 divided by 9 is roughly 7).
- `zini.DictArray` finds all distinct elements in the array, stores each once into a CompactArray (the dictionary), and creates a new CompactArray containing indexes into the dictionary.
  This will give excellent compression if there's a lot of repetition in the original array.

## Acknowledgments

Zini is merely an implementation of existing algorithms and techniques already described in the literature:

- The [PTHash][pthash] algorithm is described by Giulio Ermanno Pibiri and Roberto Trani in arXiv:2104.10402.
- They also implemented PTHash as a C++ library in <https://github.com/jermp/pthash> under the MIT license.
  Zini uses no code directly from that repository, but it has been an invaluable resource for understanding how to implement PTHash in practice.

[pthash]: https://arxiv.org/abs/2104.10402

## License

Zini is licensed under the [0BSD license](https://spdx.org/licenses/0BSD.html).
