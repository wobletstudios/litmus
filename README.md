# litmus

A test runner for Luau and Roblox projects. It comes in three parts:

- **the lib**, which specs require to register tests and make expectations
- **the CLI**, `litmus`, which runs a project's specs and prints the results
- **the Studio plugin**, which runs specs in an open place and shows the results in a window

## Backends

`litmus test` runs the project's `.spec` files in one of three places.

| Option | Where the specs run | Needs |
| --- | --- | --- |
| `--plugin` (the default) | An open Studio place, through the plugin | Studio open on the project, with the plugin switched on |
| `--cloud` | Roblox's cloud, in the place `litmus.toml` names | A `[cloud]` table and an Open Cloud API key |
| `--standalone` | A runtime on this machine, such as Lune | A `[standalone]` table |

A plugin or cloud run reads the place from the project's Rojo project, which has to describe a whole place.

## Installing

litmus installs with [Rokit](https://github.com/rojo-rbx/rokit):

```sh
rokit add wobletstudios/litmus
```

The lib is the `lib` folder of this repository. A common way to carry it is as a git submodule, with Rojo placing it in the project:

```sh
git submodule add https://github.com/wobletstudios/litmus
```

```json
"ReplicatedStorage": {
  "Litmus": { "$path": "litmus/lib" }
}
```

A plugin or cloud run looks for the lib at `ReplicatedStorage.Litmus`.

The plugin is carried inside the CLI:

```sh
litmus plugin install
```

Restart Studio after installing. After updating the CLI, install the plugin again and restart Studio, so the two speak the same protocol.

## A spec

A spec is a module whose name ends in `.spec`, as in `Cart.spec.luau`.

```luau
local cart = require("../src/Cart")
local litmus = require("../litmus/lib")

local after = litmus.after
local expect = litmus.expect
local group = litmus.group
local test = litmus.test

group("total", function(): ()
    test("adds up what the cart holds", function(): ()
        expect.equal(cart.total({ apple = 2 }, { apple = 2 }), 4)
    end)

    test("refuses what has no price", function(): ()
        expect.error(function(): ()
            cart.total({}, { plum = 1 })
        end, "nothing is priced as plum")
    end)
end)

after("leaves nothing behind", function(): ()
    expect.same(cart.open(), {})
end)
```

This spec requires the lib by its path on disk, which suits a standalone run. A spec that runs in a place requires it from where Rojo puts it, as in `require("@game/ReplicatedStorage/Litmus")`.

- `test(name, run)` registers a test. A test may yield.
- `group(name, body)` names the tests registered inside it. Groups nest.
- `after(name, run)` registers a step that runs once every test in the spec has finished. It passes or fails like a test, and goes outside every group.

### Expectations

| Call | Passes when |
| --- | --- |
| `expect.equal(actual, expected)` | the two are `==` |
| `expect.same(actual, expected)` | the two hold the same contents, however deep |
| `expect.truthy(value)` | the value is neither `nil` nor `false` |
| `expect.error(run, text?)` | `run` throws, with a message containing `text` when given |
| `expect.never.equal`, `expect.never.same`, `expect.never.truthy` | the matching expectation would fail |

### Running

```sh
litmus test                       # every spec, in Studio
litmus test tests/Cart.spec.luau  # one spec
litmus test tests/Shop            # every spec in or under a folder
litmus test --standalone
litmus test --cloud
```

`litmus --help` lists every option.

## litmus.toml

`litmus.toml` sits in the project's folder. Every table is optional; a project that only runs through the plugin needs no file at all.

```toml
[rojo]
project = "test.project.json"

[cloud]
universe = 1234567890
place = 9876543210
build = "build.rbxl"

[standalone]
run = ["lune", "run"]
lib = "litmus/lib"
specs = ["tests"]
timeout = 300
adapter = "tests/Adapter.luau"
flags = ["coverage"]
specs_at_once = 1
```

- **`[rojo]`**: `project` is the Rojo project a plugin or cloud run reads the place from, when it isn't the default one.
- **`[cloud]`**: `universe` and `place` name the experience and its place. Each run saves the project as a new, unpublished version of that place. `build` is a `.rbxl` to use as that version, for when Rojo alone doesn't build the whole place.
- **`[standalone]`** → `run` is the command that runs a Luau file, `lib` is the folder
  of litmus's lib, and `specs` is where to look for `.spec` files. `timeout` is required
  and gives the whole-runtime limit in unsigned integer seconds. Set it to `300` to keep
  the old 5-minute limit, or `0` to disable execution expiry. A positive value is one fixed
  deadline for the whole runtime, not a per-spec or idle timeout, and progress never resets
  it. Values that the platform's clock cannot represent are rejected before launch.
  Disabling expiry does not disable interruption or process-tree cleanup after completion,
  interruption, or output failure. There is no default, environment override, or timeout
  command-line flag. `adapter` and `flags` are for a project with an adapter of its own;
  `timeout` is separate from those flags and from `specs_at_once`.

  `specs_at_once` is how many specs may run at the same time. The next spec waits to load
  until one finishes. Leave it out to load every spec straight away, as before; a low limit
  keeps specs that hold a lot in memory from all holding it at once. With a limit, a run
  takes about as long as its specs one after another rather than its slowest spec, so choose
  a `timeout` that allows for that total duration. The concurrency key needs a lib from
  litmus 0.4.2 or later; an older lib ignores it and loads every spec at once.

A cloud run also needs an Open Cloud API key in `LITMUS_API_KEY`, read from the environment or else from the project's `.env`. Keep `.env` out of version control.

## Adapters and pieces

An adapter tells the lib how to run in one place: which specs there are, how to load them, and where a failure came from. The lib carries one for Roblox and one for Lune, at `lib/Adapter`.

A standalone project can wrap the Lune adapter with its own. The file `adapter` names returns a function that takes the run's options and returns an adapter:

```luau
local litmus = require("../litmus/lib")
local lune = require("../litmus/lib/Adapter/Lune")

local report = litmus.report

local function counts(specs: { litmus.SpecResult }): litmus.Remarks
    local rows: { litmus.Row } = {}

    for index, spec in specs do
        rows[index] = report.row({
            table.concat(spec.path, "/"),
            report.cell(tostring(#spec.tests), { align = "right" }),
        })
    end

    return { appendix = { report.heading("Tests in each spec"), report.tree(rows) } }
end

local function adapter(options: lune.Options): litmus.Adapter
    local adapted: litmus.Adapter = lune(options)

    if options.flags.counts then
        adapted.results = counts
    end

    return adapted
end

return adapter
```

An adapter's `results` runs once every spec has finished. What it returns can add to the run in two ways:

- `appendix`: a list of pieces, shown under the specs in the terminal and in the plugin's window
- `failure`: a reason to fail the run whatever its tests did, such as coverage under a floor

Each flag listed under `flags` becomes an option of `litmus test --standalone` and reaches the adapter in `options.flags`, as in `litmus test --standalone --counts`.

`examples/standalone` is a whole project set up this way.

### Pieces

`litmus.report` makes the pieces of an appendix.

| Call | Makes |
| --- | --- |
| `report.heading(text)` | a heading |
| `report.text(text)` | plain text |
| `report.stats({ { label, text }, ... })` | labelled lines |
| `report.legend({ { label, tone }, ... })` | a key to the tones a tree uses |
| `report.tree(rows)` | a table of rows, which can hold rows of their own |
| `report.row(cells, { open?, rows? })` | a row; a cell is a string, a `report.cell` or a `report.meter` |
| `report.cell(text, { align?, tone? })` | a cell with an align of `left` or `right` |
| `report.meter(parts)` | a bar of up to 10 parts |
| `report.part(value, tone)` | one part of a meter |

A tone is `good`, `plain` or `dim`.

## Working on litmus

The tools are pinned in `rokit.toml`, and the CLI needs a Rust toolchain.

```sh
rokit install
lune run dev --open   # build the place, open it in Studio, and serve it with Rojo
```

### Checks

```sh
cd cli && cargo test                        # the CLI's own tests
cli/target/debug/litmus test --standalone   # the specs, under Lune
cli/target/debug/litmus test                # the specs, in Studio
lune run analyze                            # type-check the lib, the tests and the plugin
```

`lune run analyze --full` prints each problem whole, where the plain command cuts long lines short. The Roblox type definitions it checks against are fetched into `luau-cache`; delete that folder to fetch newer ones.

### The Core check

The specs test the lib with the lib, so a broken lib could pass its own tests. The Core check runs one spec through the lib and compares what comes back against a list written out by hand, using nothing of litmus to do the comparing. Run it from Studio's command bar:

```luau
loadstring(game.ReplicatedStorage.Tests.Core.Source)()
```

It prints `[Core]: all 26 tests match`, or warns of each mismatch.

## License

MIT. See [LICENSE](LICENSE).
