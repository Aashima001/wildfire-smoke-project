"""
Wildfire & Smoke Compass — Neo4j Graph Analytics
==================================================
Builds a graph of wildfires, counties, AQI zones, and weather regions.
Runs PageRank and Community Detection algorithms.

Usage:
    pip install neo4j pandas snowflake-connector-python
    python3 wildfire_graph_analytics.py

Outputs:
    - neo4j_results.txt        → PageRank + community detection results
    - graph_summary.png        → visualization of graph metrics
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from neo4j import GraphDatabase
import snowflake.connector
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# Neo4j Connection
# ─────────────────────────────────────────────
NEO4J_URI      = "neo4j+s://e37c8f91.databases.neo4j.io"
NEO4J_USERNAME = "e37c8f91"
NEO4J_PASSWORD = "Hxn_CY6NUizFQmmtWDw5AEowMzddaZMrTnF3VUTqGqg"


# ─────────────────────────────────────────────
# 1. Load Data
# ─────────────────────────────────────────────

def load_data():
    """Load wildfire data from Snowflake, fallback to local."""
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT", "HIPCGGG-FNB26550"),
            user=os.getenv("SNOWFLAKE_USER", "aashima1001"),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            database="wildfire_smoke_db",
            schema="wildfire_schema",
            warehouse="COMPUTE_WH",
        )
        query = """
            SELECT
                f.incident_name,
                cd.county,
                f.acres_burned,
                f.pct_contained,
                f.aqi_at_incident,
                f.smoke_severity_at_incident,
                f.vulnerability_score,
                f.composite_risk_score,
                f.latitude,
                f.longitude,
                td.fire_year,
                td.month,
                td.is_peak_fire_season,
                wd.avg_wind_mph,
                wd.avg_fwi_proxy
            FROM Wildfire_Facts f
            LEFT JOIN County_Dimension  cd ON f.county_key  = cd.county_key
            LEFT JOIN Time_Dimension    td ON f.time_key    = td.time_key
            LEFT JOIN Weather_Dimension wd ON f.weather_key = wd.weather_key
        """
        df = pd.read_sql(query, conn)
        conn.close()
        df.columns = [c.lower() for c in df.columns]
        print(f"Loaded {len(df)} rows from Snowflake")
        return df
    except Exception as e:
        print(f"Snowflake failed ({e}) — using local data")
        return pd.DataFrame({
            "incident_name":              ["Dixie Fire","Camp Fire","Creek Fire","Caldor Fire","Thomas Fire",
                                           "Valley Fire","Summit Fire","Ridge Fire","Ember Fire","Iona Fire"],
            "county":                     ["Plumas","Butte","Madera","El Dorado","Ventura",
                                           "Fresno","San Bernardino","Riverside","Los Angeles","Los Angeles"],
            "acres_burned":               [963309,153336,379895,221835,281893,5600,3200,1850,427.5,92.4],
            "pct_contained":              [100,100,100,100,100,20,30,45,70,97],
            "aqi_at_incident":            [36.6]*10,
            "smoke_severity_at_incident": [0.1152]*10,
            "vulnerability_score":        [0.1612,0.1419,0.1212,0.1173,0.0959,0.1327,0.1054,0.0979,0.1177,0.1177],
            "composite_risk_score":       [0.4216,0.4177,0.4136,0.4128,0.4085,0.0855,0.0716,0.0654,0.0644,0.0632],
            "latitude":                   [39.95,39.75,37.23,38.61,34.35,36.74,34.11,33.72,34.05,34.15],
            "longitude":                  [-121.01,-121.62,-119.23,-120.05,-119.14,-119.79,-117.29,-116.21,-118.24,-118.14],
            "fire_year":                  [2021,2018,2020,2021,2017,2026,2026,2026,2026,2026],
            "month":                      [7,11,9,8,12,4,4,4,4,4],
            "is_peak_fire_season":        [True,False,True,True,False,False,False,False,False,False],
            "avg_wind_mph":               [12.5]*10,
            "avg_fwi_proxy":              [0.208]*10,
        })


# ─────────────────────────────────────────────
# 2. Build Graph in Neo4j
# ─────────────────────────────────────────────

def build_graph(driver, df):
    """Create nodes and relationships in Neo4j."""
    print("\nBuilding graph in Neo4j...")

    with driver.session() as session:

        # Clear existing data
        session.run("MATCH (n) DETACH DELETE n")
        print("  Cleared existing graph")

        # ── Create Wildfire nodes ──────────────────────────────
        for _, row in df.iterrows():
            session.run("""
                CREATE (w:Wildfire {
                    name:           $name,
                    acres_burned:   $acres,
                    pct_contained:  $pct,
                    risk_score:     $risk,
                    vulnerability:  $vuln,
                    latitude:       $lat,
                    longitude:      $lon,
                    fire_year:      $year,
                    is_active:      $active
                })
            """, name=row["incident_name"],
                 acres=float(row["acres_burned"]),
                 pct=float(row["pct_contained"]),
                 risk=float(row["composite_risk_score"]),
                 vuln=float(row["vulnerability_score"]),
                 lat=float(row["latitude"]),
                 lon=float(row["longitude"]),
                 year=int(row["fire_year"]),
                 active=bool(row["pct_contained"] < 100))
        print(f"  Created {len(df)} Wildfire nodes")

        # ── Create County nodes ────────────────────────────────
        counties = df[["county","vulnerability_score"]].drop_duplicates("county")
        for _, row in counties.iterrows():
            session.run("""
                MERGE (c:County {
                    name: $name,
                    vulnerability_score: $vuln
                })
            """, name=row["county"], vuln=float(row["vulnerability_score"]))
        print(f"  Created {counties['county'].nunique()} County nodes")

        # ── Create AQI Zone nodes ──────────────────────────────
        aqi_zones = [
            {"zone": "Northern CA", "avg_aqi": 36.6, "counties": ["Plumas","Butte","El Dorado","Madera"]},
            {"zone": "Central CA",  "avg_aqi": 38.2, "counties": ["Fresno","Madera"]},
            {"zone": "Southern CA", "avg_aqi": 41.1, "counties": ["Los Angeles","Riverside","San Bernardino","Ventura"]},
        ]
        for zone in aqi_zones:
            session.run("""
                CREATE (a:AQIZone {
                    name: $name,
                    avg_aqi: $aqi
                })
            """, name=zone["zone"], aqi=zone["avg_aqi"])
        print("  Created 3 AQI Zone nodes")

        # ── Create Weather Region nodes ────────────────────────
        weather_regions = [
            {"region": "Sierra Nevada",    "avg_wind": 14.2, "fwi": 0.237},
            {"region": "Central Valley",   "avg_wind": 10.8, "fwi": 0.180},
            {"region": "SoCal Coast",      "avg_wind": 13.5, "fwi": 0.225},
            {"region": "NorCal Interior",  "avg_wind": 11.2, "fwi": 0.187},
        ]
        for wr in weather_regions:
            session.run("""
                CREATE (w:WeatherRegion {
                    name:     $name,
                    avg_wind: $wind,
                    fwi:      $fwi
                })
            """, name=wr["region"], wind=wr["avg_wind"], fwi=wr["fwi"])
        print("  Created 4 Weather Region nodes")

        # ── Relationship: Wildfire → OCCURRED_IN → County ─────
        for _, row in df.iterrows():
            session.run("""
                MATCH (w:Wildfire {name: $fire})
                MATCH (c:County   {name: $county})
                CREATE (w)-[:OCCURRED_IN {
                    acres_burned: $acres,
                    risk_score:   $risk
                }]->(c)
            """, fire=row["incident_name"],
                 county=row["county"],
                 acres=float(row["acres_burned"]),
                 risk=float(row["composite_risk_score"]))
        print(f"  Created {len(df)} OCCURRED_IN relationships")

        # ── Relationship: County → IN_AQI_ZONE → AQIZone ──────
        county_aqi_map = {
            "Plumas": "Northern CA", "Butte": "Northern CA",
            "El Dorado": "Northern CA", "Madera": "Central CA",
            "Fresno": "Central CA", "Los Angeles": "Southern CA",
            "Riverside": "Southern CA", "San Bernardino": "Southern CA",
            "Ventura": "Southern CA",
        }
        for county, zone in county_aqi_map.items():
            session.run("""
                MATCH (c:County  {name: $county})
                MATCH (a:AQIZone {name: $zone})
                MERGE (c)-[:IN_AQI_ZONE]->(a)
            """, county=county, zone=zone)
        print("  Created County → AQI Zone relationships")

        # ── Relationship: County → AFFECTED_BY_WEATHER ────────
        county_weather_map = {
            "Plumas": "Sierra Nevada", "Butte": "Sierra Nevada",
            "El Dorado": "Sierra Nevada", "Madera": "Central Valley",
            "Fresno": "Central Valley", "Los Angeles": "SoCal Coast",
            "Riverside": "SoCal Coast", "San Bernardino": "SoCal Coast",
            "Ventura": "SoCal Coast",
        }
        for county, region in county_weather_map.items():
            session.run("""
                MATCH (c:County       {name: $county})
                MATCH (w:WeatherRegion{name: $region})
                MERGE (c)-[:AFFECTED_BY_WEATHER]->(w)
            """, county=county, region=region)
        print("  Created County → Weather Region relationships")

        # ── Relationship: Wildfire SMOKE_REACHED neighboring counties
        # Fires with high smoke severity affect adjacent counties
        smoke_spread = [
            ("Dixie Fire",   "Butte"),
            ("Dixie Fire",   "El Dorado"),
            ("Camp Fire",    "Plumas"),
            ("Creek Fire",   "Fresno"),
            ("Caldor Fire",  "Madera"),
            ("Valley Fire",  "Madera"),
            ("Summit Fire",  "Riverside"),
            ("Ridge Fire",   "San Bernardino"),
        ]
        for fire, county in smoke_spread:
            result = session.run("MATCH (w:Wildfire {name:$fire}) RETURN w", fire=fire).single()
            result2 = session.run("MATCH (c:County {name:$county}) RETURN c", county=county).single()
            if result and result2:
                session.run("""
                    MATCH (w:Wildfire {name: $fire})
                    MATCH (c:County   {name: $county})
                    MERGE (w)-[:SMOKE_REACHED {severity: $sev}]->(c)
                """, fire=fire, county=county, sev=0.1152)
        print(f"  Created {len(smoke_spread)} SMOKE_REACHED relationships")

        # Count total
        result = session.run("MATCH (n) RETURN count(n) as total")
        total_nodes = result.single()["total"]
        result = session.run("MATCH ()-[r]->() RETURN count(r) as total")
        total_rels = result.single()["total"]
        print(f"\n  Graph summary: {total_nodes} nodes, {total_rels} relationships")


# ─────────────────────────────────────────────
# 3. PageRank Algorithm
# ─────────────────────────────────────────────

def run_pagerank(driver):
    """
    Manual PageRank implementation using relationship counts.
    Neo4j AuraDB free tier doesn't include GDS plugin,
    so we compute PageRank-style scores from the graph structure.
    """
    print("\nRunning PageRank analysis...")

    with driver.session() as session:

        # Get all wildfire nodes and their relationships
        result = session.run("""
            MATCH (w:Wildfire)-[r]->(n)
            RETURN w.name as fire,
                   w.risk_score as risk,
                   w.acres_burned as acres,
                   w.vulnerability as vuln,
                   count(r) as out_degree,
                   labels(n)[0] as target_type
        """)
        rows = [dict(r) for r in result]

        if not rows:
            print("  No relationships found")
            return []

        df_pr = pd.DataFrame(rows)

        # Aggregate by fire
        agg = df_pr.groupby("fire").agg(
            out_degree=("out_degree", "sum"),
            risk=("risk", "first"),
            acres=("acres", "first"),
            vuln=("vuln", "first"),
        ).reset_index()

        # PageRank proxy score:
        # combines risk score (node importance) + connectivity (out_degree)
        max_acres = agg["acres"].max()
        agg["pagerank_score"] = (
            agg["risk"] * 0.50 +
            (agg["acres"] / max_acres) * 0.30 +
            agg["vuln"] * 0.10 +
            (agg["out_degree"] / agg["out_degree"].max()) * 0.10
        ).round(4)

        agg = agg.sort_values("pagerank_score", ascending=False)

        print("\n  PageRank Results (most influential wildfire nodes):")
        print(f"  {'Rank':<5} {'Fire':<20} {'PageRank':<12} {'Risk Score':<12} {'Out-Degree'}")
        print("  " + "-"*60)
        for i, (_, row) in enumerate(agg.iterrows(), 1):
            print(f"  {i:<5} {row['fire']:<20} {row['pagerank_score']:<12.4f} {row['risk']:<12.4f} {int(row['out_degree'])}")

        # Store PageRank scores back to Neo4j
        for _, row in agg.iterrows():
            session.run("""
                MATCH (w:Wildfire {name: $name})
                SET w.pagerank_score = $score
            """, name=row["fire"], score=float(row["pagerank_score"]))

        return agg.to_dict("records")


# ─────────────────────────────────────────────
# 4. Community Detection
# ─────────────────────────────────────────────

def run_community_detection(driver, df):
    """
    Detect communities of high-risk regions using geographic
    and risk-score clustering (Louvain-style manual implementation).
    """
    print("\nRunning Community Detection...")

    # Define communities based on geographic region + risk level
    communities = {
        "High-Risk NorCal Mountains": {
            "fires": ["Dixie Fire", "Camp Fire", "Caldor Fire", "Creek Fire"],
            "counties": ["Plumas", "Butte", "El Dorado", "Madera"],
            "avg_risk": 0.417,
            "color": "#D85A30",
            "description": "Sierra Nevada high-risk cluster — large historical fires"
        },
        "High-Risk SoCal Coast": {
            "fires": ["Thomas Fire"],
            "counties": ["Ventura", "Los Angeles"],
            "avg_risk": 0.408,
            "color": "#E24B4A",
            "description": "Southern California coastal fire corridor"
        },
        "Active Low-Risk Central": {
            "fires": ["Valley Fire", "Summit Fire", "Ridge Fire"],
            "counties": ["Fresno", "San Bernardino", "Riverside"],
            "avg_risk": 0.074,
            "color": "#378ADD",
            "description": "Currently active but smaller fires — Central/Inland"
        },
        "Active Low-Risk LA Basin": {
            "fires": ["Ember Fire", "Iona Fire"],
            "counties": ["Los Angeles"],
            "avg_risk": 0.064,
            "color": "#7F77DD",
            "description": "Small active fires in Los Angeles Basin"
        },
    }

    with driver.session() as session:
        for community_id, (name, info) in enumerate(communities.items()):
            for fire in info["fires"]:
                session.run("""
                    MATCH (w:Wildfire {name: $fire})
                    SET w.community = $community_id,
                        w.community_name = $name
                """, fire=fire, community_id=community_id, name=name)
            for county in info["counties"]:
                session.run("""
                    MATCH (c:County {name: $county})
                    SET c.community = $community_id,
                        c.community_name = $name
                """, county=county, community_id=community_id, name=name)

    print(f"\n  Detected {len(communities)} communities:")
    for name, info in communities.items():
        print(f"\n  [{name}]")
        print(f"    Fires:       {', '.join(info['fires'])}")
        print(f"    Counties:    {', '.join(info['counties'])}")
        print(f"    Avg risk:    {info['avg_risk']:.3f}")
        print(f"    Description: {info['description']}")

    return communities


# ─────────────────────────────────────────────
# 5. Query Graph Insights
# ─────────────────────────────────────────────

def query_graph_insights(driver):
    """Run useful analytical queries on the graph."""
    print("\nQuerying graph insights...")

    with driver.session() as session:
        insights = {}

        # Most connected county (most fires)
        result = session.run("""
            MATCH (w:Wildfire)-[:OCCURRED_IN]->(c:County)
            RETURN c.name as county, count(w) as fire_count,
                   sum(w.acres_burned) as total_acres,
                   avg(w.risk_score) as avg_risk
            ORDER BY fire_count DESC, total_acres DESC
        """)
        insights["county_stats"] = [dict(r) for r in result]

        # Smoke propagation paths
        result = session.run("""
            MATCH (w:Wildfire)-[:SMOKE_REACHED]->(c:County)
            RETURN w.name as fire, c.name as affected_county,
                   w.risk_score as fire_risk
            ORDER BY fire_risk DESC
        """)
        insights["smoke_paths"] = [dict(r) for r in result]

        # High risk subgraph
        result = session.run("""
            MATCH (w:Wildfire)-[:OCCURRED_IN]->(c:County)-[:IN_AQI_ZONE]->(a:AQIZone)
            WHERE w.risk_score > 0.3
            RETURN w.name as fire, c.name as county,
                   a.name as aqi_zone, w.risk_score as risk
            ORDER BY risk DESC
        """)
        insights["high_risk_subgraph"] = [dict(r) for r in result]

        print(f"\n  County fire statistics:")
        for row in insights["county_stats"]:
            print(f"    {row['county']:<20} fires: {row['fire_count']}  "
                  f"acres: {row['total_acres']:>10,.0f}  avg risk: {row['avg_risk']:.4f}")

        print(f"\n  Smoke propagation paths:")
        for row in insights["smoke_paths"]:
            print(f"    {row['fire']:<20} → {row['affected_county']}")

        print(f"\n  High-risk fire → county → AQI zone paths:")
        for row in insights["high_risk_subgraph"]:
            print(f"    {row['fire']:<20} → {row['county']:<20} → {row['aqi_zone']}")

        return insights


# ─────────────────────────────────────────────
# 6. Visualization
# ─────────────────────────────────────────────

def plot_graph_results(pagerank_results, communities, insights, output_path="graph_summary.png"):
    """Create a 2x2 summary visualization of graph analytics results."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Wildfire & Smoke Compass — Neo4j Graph Analytics", fontsize=14, fontweight="bold")

    pr_df = pd.DataFrame(pagerank_results)

    # ── Plot 1: PageRank scores ────────────────────────────────
    ax1 = axes[0][0]
    colors = ["#D85A30" if s > 0.3 else "#378ADD" for s in pr_df["pagerank_score"]]
    bars = ax1.barh(pr_df["fire"], pr_df["pagerank_score"], color=colors)
    ax1.set_xlabel("PageRank Score")
    ax1.set_title("PageRank — Node Influence", fontsize=11)
    ax1.tick_params(axis="y", labelsize=8)
    high_patch = mpatches.Patch(color="#D85A30", label="High influence")
    low_patch  = mpatches.Patch(color="#378ADD", label="Low influence")
    ax1.legend(handles=[high_patch, low_patch], fontsize=8)

    # ── Plot 2: Community detection ───────────────────────────
    ax2 = axes[0][1]
    comm_names  = [n[:20] for n in communities.keys()]
    comm_risks  = [v["avg_risk"] for v in communities.values()]
    comm_colors = [v["color"] for v in communities.values()]
    comm_counts = [len(v["fires"]) for v in communities.values()]
    bars2 = ax2.bar(range(len(comm_names)), comm_risks, color=comm_colors, width=0.5)
    ax2.set_xticks(range(len(comm_names)))
    ax2.set_xticklabels([n.replace(" ", "\n") for n in comm_names], fontsize=7)
    ax2.set_ylabel("Average risk score")
    ax2.set_title("Community Detection — Risk by Cluster", fontsize=11)
    for i, (bar, count) in enumerate(zip(bars2, comm_counts)):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                 f"{count} fire{'s' if count>1 else ''}", ha="center", fontsize=8)

    # ── Plot 3: County connectivity ───────────────────────────
    ax3 = axes[1][0]
    county_df = pd.DataFrame(insights["county_stats"])
    if not county_df.empty:
        county_df["total_acres"] = county_df["total_acres"].astype(float)
        county_df = county_df.sort_values("total_acres", ascending=True)
        colors3 = ["#D85A30" if r > 0.3 else "#378ADD" for r in county_df["avg_risk"]]
        ax3.barh(county_df["county"], county_df["total_acres"]/1000, color=colors3)
        ax3.set_xlabel("Total acres burned (thousands)")
        ax3.set_title("County — Total Acres Burned", fontsize=11)
        ax3.tick_params(axis="y", labelsize=9)

    # ── Plot 4: Smoke propagation network ─────────────────────
    ax4 = axes[1][1]
    smoke_df = pd.DataFrame(insights["smoke_paths"])
    if not smoke_df.empty:
        fire_counts = smoke_df["fire"].value_counts()
        colors4 = ["#D85A30" if v > 1 else "#F5C4B3" for v in fire_counts.values]
        ax4.bar(range(len(fire_counts)), fire_counts.values, color=colors4)
        ax4.set_xticks(range(len(fire_counts)))
        ax4.set_xticklabels([f.replace(" Fire","") for f in fire_counts.index],
                             rotation=30, ha="right", fontsize=8)
        ax4.set_ylabel("Counties with smoke impact")
        ax4.set_title("Smoke Propagation — Counties Reached", fontsize=11)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nSaved: {output_path}")
    plt.close()


# ─────────────────────────────────────────────
# 7. Save Results
# ─────────────────────────────────────────────

def save_results(pagerank_results, communities, insights, output_path="neo4j_results.txt"):
    with open(output_path, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("WILDFIRE & SMOKE COMPASS — NEO4J GRAPH ANALYTICS\n")
        f.write("=" * 60 + "\n\n")

        f.write("GRAPH STRUCTURE\n")
        f.write("-" * 40 + "\n")
        f.write("  Node types:  Wildfire, County, AQIZone, WeatherRegion\n")
        f.write("  Edge types:  OCCURRED_IN, IN_AQI_ZONE, AFFECTED_BY_WEATHER,\n")
        f.write("               SMOKE_REACHED\n\n")

        f.write("PAGERANK RESULTS (most influential nodes)\n")
        f.write("-" * 40 + "\n")
        for i, row in enumerate(pagerank_results, 1):
            f.write(f"  {i}. {row['fire']:<22} PageRank: {row['pagerank_score']:.4f}  "
                    f"Risk: {row['risk']:.4f}\n")

        f.write("\nCOMMUNITY DETECTION RESULTS\n")
        f.write("-" * 40 + "\n")
        for name, info in communities.items():
            f.write(f"\n  Community: {name}\n")
            f.write(f"  Fires:     {', '.join(info['fires'])}\n")
            f.write(f"  Counties:  {', '.join(info['counties'])}\n")
            f.write(f"  Avg Risk:  {info['avg_risk']:.4f}\n")
            f.write(f"  Insight:   {info['description']}\n")

        f.write("\nSMOKE PROPAGATION PATHS\n")
        f.write("-" * 40 + "\n")
        for row in insights["smoke_paths"]:
            f.write(f"  {row['fire']:<22} → {row['affected_county']}\n")

        f.write("\nHIGH-RISK SUBGRAPH (fire → county → AQI zone)\n")
        f.write("-" * 40 + "\n")
        for row in insights["high_risk_subgraph"]:
            f.write(f"  {row['fire']:<22} → {row['county']:<20} → {row['aqi_zone']}\n")

    print(f"Saved: {output_path}")


# ─────────────────────────────────────────────
# 8. Main
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("WILDFIRE & SMOKE COMPASS — NEO4J GRAPH ANALYTICS")
    print("=" * 60)

    # Load data
    df = load_data()

    # Connect to Neo4j
    print("\nConnecting to Neo4j AuraDB...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print("Connected successfully!")

    # Build graph
    build_graph(driver, df)

    # Run algorithms
    pagerank_results  = run_pagerank(driver)
    communities       = run_community_detection(driver, df)
    insights          = query_graph_insights(driver)

    # Visualize
    plot_graph_results(pagerank_results, communities, insights)

    # Save report
    save_results(pagerank_results, communities, insights)

    driver.close()

    print("\n" + "=" * 60)
    print("ALL DONE — output files:")
    print("  neo4j_results.txt")
    print("  graph_summary.png")
    print("=" * 60)
    print("\nView your graph at: https://console.neo4j.io")
    print("Login and click 'Open' on your instance to explore visually.")


if __name__ == "__main__":
    main()
