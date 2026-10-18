# Frames written by 1.0.0

What a node of 1.0.0 sends to another node, one file per message (`<name>.bin`), written by 1.0.0's own code
(`v1.0.0` with the writer `docs/compat/WriteFramesOf100.scala`, 18/10/2026). `index.tsv`: name, serializer id and
manifest, or `unsendable` and the error for a value 1.0.0 could not send at all. `FramesOf100Spec` reads every frame with
the current code: a node of this version must read what a 1.0.0 node sends during a rolling upgrade.
