# Zini

Zini (Zig + Mini) is a [Zig](https://ziglang.org/) library providing some succinct data structures.

The main contribution is `zini.pthash` which is an implementation of [PTHash][pthash], a [minimal perfect hash function](https://en.wikipedia.org/wiki/Perfect_hash_function) construction algorithm.
Given a set of `n` elements, with the only requirement being that you can hash them, it generates a hash function which maps each element to a distinct number between `0` and `n - 1`.
The generated hash function is extremely small, typically consuming less than **4 _bits_ per element**, regardless of the size of the input type.
The algorithm provides multiple parameters to tune making it possible to optimize for (small) size, (short) construction time, or (short) lookup time.

To give a practical example:
In ~0.6 seconds Zini was able to create a hash function for /usr/share/dict/words containing 235886 words.
The resulting hash function required in total 865682 bits in memory.
This corresponds to 108.2 kB in total or 3.67 bits per word.
In comparison, the original file was 2.49 MB and compressing it with `gzip -9` only gets it down to 754 kB (which you can't use directly in memory without decompressing it).
It should of course be noted that they don't store the equivalent data as you can't use the generated hash function to determine if a word is present or not in the list.
The comparison is mainly useful to get a feeling of the magnitudes.

In addition, Zini provides various functionality for dealing with arrays of numbers:

- `zini.CompactArray` stores n-bit numbers tightly packed, leaving no bits unused.
  If the largest value in an array is `m` then you actually only need `n = log2(m) + 1` bits per element.
  E.g. if the largest value is 270, you will get 7x compression using CompactArray over `[]u64` as it stores each element using only 9 bits (and 64 divided by 9 is roughly 7).
- `zini.DictArray` finds all distinct elements in the array, stores each once into a CompactArray (the dictionary), and creates a new CompactArray containing indexes into the dictionary.
  This will give excellent compression if there's a lot of repetition in the original array.
- `zini.EliasFano` stores increasing 64-bit numbers in a compact manner.
- `zini.darray` provides constant-time support for the `select1(i)` operation which returns the _i_-th set bit in a `std.DynamicBitSetUnmanaged`.

## Usage

Zini is intended to be used as a library, but also ships the command-line tool `zini-pthash`.
As the documentation is a bit lacking it might be useful to look through `tools/zini-pthash/main.zig` to understand how it's used.

Note that building `zini-pthash` depends on having [parg](https://github.com/judofyr/parg) cloned in `../parg`.
You may want to tweak this in `build.zig`.

```
USAGE
  ./zig-out/bin/zini-pthash [build | lookup] <options>

COMMAND: build
  Builds hash function for plain text file.

  -i, --input <file>
  -o, --output <file>
  -c <int>
  -a, --alpha <float>
  -s, --seed <int>

COMMAND: lookup

  -i, --input <file>
  -k, --key <key>
  -b, --benchmark
```

And here's an example run of using `zini-pthash`.

```
# Build zini-pthash:
$ zig build -Drelease-safe

# Build a hash function:
$ ./zig-out/bin/zini-pthash build -i /usr/share/dict/words -o words.pth
Reading /usr/share/dict/words...

Building hash function...

Successfully built hash function:
  seed: 12323441790160983030
  bits: 865554
  bits/n: 3.6693741892269993

Writing to words.pth

# Look up an index in the hash function:
$ ./zig-out/bin/zini-pthash lookup -i words.pth --key hello
Reading words.pth...

Successfully loaded hash function:
  seed: 12323441790160983030
  bits: 865554
  bits/n: 3.6693741892269993

Looking up key=hello:
112576
```

## Acknowledgments

Zini is merely an implementation of existing algorithms and techniques already described in the literature:

- The [PTHash][pthash] algorithm is described by Giulio Ermanno Pibiri and Roberto Trani in arXiv:2104.10402.
- They also implemented PTHash as a C++ library in <https://github.com/jermp/pthash> under the MIT license.
  Zini uses no code directly from that repository, but it has been an invaluable resource for understanding how to implement PTHash in practice.

[pthash]: https://arxiv.org/abs/2104.10402

## License

Zini is licensed under the [0BSD license](https://spdx.org/licenses/0BSD.html).
