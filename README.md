coninterner
============

This library provides an concurrent add only interner that looks only the value with the same hash during interning.
while resize only add of new values is blocked until the resize is done.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
coninterner = "0.1"
```

## License

Licensed under:
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
