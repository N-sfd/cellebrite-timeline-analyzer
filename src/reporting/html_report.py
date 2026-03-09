from jinja2 import Template
import pandas as pd

HTML_TEMPLATE = """
<h1>Cellebrite Timeline Report</h1>

<p><b>Total Events:</b> {{total}}</p>

<h2>Event Breakdown</h2>
<ul>
{% for k,v in actions.items() %}
<li>{{k}} : {{v}}</li>
{% endfor %}
</ul>

<h2>Actor Analysis</h2>
<ul>
{% for k,v in actors.items() %}
<li>{{k}} : {{v}}</li>
{% endfor %}
</ul>

<h2>Timeline Sample</h2>
{{table}}
"""

def create_html_report(df, output_file):

    actions = df["action"].value_counts().to_dict()
    actors = df["actor_hint"].value_counts().to_dict()

    table = df.head(50).to_html()

    html = Template(HTML_TEMPLATE).render(
        total=len(df),
        actions=actions,
        actors=actors,
        table=table
    )

    with open(output_file,"w") as f:
        f.write(html)