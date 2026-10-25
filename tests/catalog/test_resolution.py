from resolve_entities import best_edge_per_package, connected_components, edges_fuzzy, edges_shared_repo

CAND = ("candidate_id string, source string, category string, name string, norm_name string, "
        "publisher string, repo string, declared_packages array<string>")


def cands(spark, rows):
    return spark.createDataFrame(rows, CAND)


def pairs(df):
    return sorted((r.src, r.dst) for r in df.collect())


def test_shared_repo_links_one_server_and_its_package(spark):
    df = cands(spark, [
        ("mcp_registry:acme/pg", "mcp_registry", "mcp_server", "acme/pg", "pg", "acme", "acme/pg-mcp", None),
        ("npm:acme-pg-mcp", "npm", "package", "acme-pg-mcp", "acmepg", "acme", "acme/pg-mcp", None),
    ])
    assert pairs(edges_shared_repo(df)) == [("mcp_registry:acme/pg", "npm:acme-pg-mcp")]


def test_monorepo_guard_blocks_shared_repo(spark):
    # modelcontextprotocol/servers hosts many servers: sharing it must not merge them.
    repo = "modelcontextprotocol/servers"
    df = cands(spark, [
        ("mcp_registry:a/filesystem", "mcp_registry", "mcp_server", "a/filesystem", "filesystem", "a", repo, None),
        ("mcp_registry:a/git", "mcp_registry", "mcp_server", "a/git", "git", "a", repo, None),
        ("npm:@modelcontextprotocol/server-filesystem", "npm", "package", "x", "filesystem", "modelcontextprotocol", repo, None),
    ])
    assert pairs(edges_shared_repo(df)) == []


def test_fuzzy_needs_more_than_a_similar_name(spark):
    df = cands(spark, [
        ("mcp_registry:com.picsart.api/gen-ai", "mcp_registry", "mcp_server", "com.picsart.api/gen-ai", "genai", "com.picsart.api", None, None),
        ("npm:@google/genai", "npm", "package", "@google/genai", "genai", "google", "googleapis/js-genai", None),
        ("mcp_registry:io.github.acme/weather", "mcp_registry", "mcp_server", "io.github.acme/weather", "weather", "acme", None, None),
        ("npm:weather-mcp", "npm", "package", "weather-mcp", "weather", "someone", None, None),
    ])
    # The SDK is not an MCP package and has a different publisher; weather-mcp is an MCP package.
    assert pairs(edges_fuzzy(df)) == [("mcp_registry:io.github.acme/weather", "npm:weather-mcp")]


def test_each_package_attaches_to_one_server(spark):
    edges = spark.createDataFrame([
        ("mcp_registry:gaudiolab/dev", "npm:@gaudiolab/mcp-developers", "declared_package"),
        ("mcp_registry:jocktaxes/developer", "npm:@gaudiolab/mcp-developers", "fuzzy_name"),
    ], "src string, dst string, rule string")
    kept = best_edge_per_package(edges)
    assert pairs(kept) == [("mcp_registry:gaudiolab/dev", "npm:@gaudiolab/mcp-developers")]

    nodes = spark.createDataFrame([
        ("mcp_registry:gaudiolab/dev", "mcp_registry", "mcp_server"),
        ("mcp_registry:jocktaxes/developer", "mcp_registry", "mcp_server"),
        ("npm:@gaudiolab/mcp-developers", "npm", "package"),
    ], "candidate_id string, source string, category string")
    labels = {r.candidate_id: r.label[1:] for r in connected_components(nodes, kept).collect()}
    assert labels["npm:@gaudiolab/mcp-developers"] == "mcp_registry:gaudiolab/dev"
    assert labels["mcp_registry:jocktaxes/developer"] == "mcp_registry:jocktaxes/developer"
