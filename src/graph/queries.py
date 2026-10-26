"""Build bounded SPARQL queries using explicit named graphs and source IDs."""

from graph.domain.source import PostReference
from graph.mapping.identity import NAMESPACE, IriPolicy


def select(projection: str, body: str, graph_keys: tuple[str, ...], limit: int) -> str:
    """Build a distinct SELECT with explicit named-graph scope and row limit."""
    if not graph_keys or not 1 <= limit <= 10000:
        raise ValueError("Queries require graph scope and a row limit between 1 and 10000")
    iris = IriPolicy()
    graphs = " ".join(f"<{iris.graph(key)}>" for key in graph_keys)
    return f"PREFIX so: <{NAMESPACE}>\nSELECT DISTINCT {projection} WHERE {{ VALUES ?g {{ {graphs} }} GRAPH ?g {{ {body} }} }} LIMIT {limit}"


def linked_target_bodies(
    source_graph: str, target_graphs: tuple[str, ...], source_id: int, target_id: int, limit: int
) -> str:
    """Build a query for linked target bodies across bounded target graphs."""
    iris = IriPolicy()
    source, target = iris.question(source_id), iris.question(target_id)
    body = f"GRAPH <{iris.graph(source_graph)}> {{ <{source}> so:linkedTo <{target}> }} GRAPH ?g {{ <{target}> so:hasBody ?body }}"
    graphs = " ".join(f"<{iris.graph(key)}>" for key in target_graphs)
    if not target_graphs or not 1 <= limit <= 10000:
        raise ValueError("Queries require bounded target graphs")
    return f"PREFIX so: <{NAMESPACE}>\nSELECT DISTINCT ?body WHERE {{ VALUES ?g {{ {graphs} }} {body} }} LIMIT {limit}"


def distinct_source_ids(graphs: tuple[str, ...], ids: tuple[int, ...], limit: int) -> str:
    """Build a query for source IDs present in the selected graphs."""
    iris = IriPolicy()
    subjects = " ".join(f"<{iris.question(source_id)}>" for source_id in ids)
    return select(
        "?id", f"VALUES ?question {{ {subjects} }} ?question a so:Question ; so:sourcePostId ?id .", graphs, limit
    )


def edge(graph: str, source_id: int, target_id: int, predicate: str) -> str:
    """Build a query for one linkedTo or duplicateOf edge."""
    if predicate not in ("linkedTo", "duplicateOf"):
        raise ValueError("Only question link and duplicate relations are supported")
    iris = IriPolicy()
    return select(
        "?target",
        f"<{iris.question(source_id)}> so:{predicate} ?target . FILTER (?target = <{iris.question(target_id)}>)",
        (graph,),
        1,
    )


def accepted_answer(graph: str, question_id: int) -> str:
    """Build a query for an accepted answer and its optional body and score."""
    return select(
        "?answer ?body ?score",
        f"<{IriPolicy().question(question_id)}> so:acceptedAnswer ?answer . OPTIONAL {{ ?answer so:hasBody ?body }} OPTIONAL {{ ?answer so:score ?score }}",
        (graph,),
        2,
    )


def answer_comment(graph: str, question_id: int, answer_id: int, comment_id: int) -> str:
    """Build a query for an answer's body and one comment's text and score."""
    iris = IriPolicy()
    question, answer = iris.question(question_id), iris.answer(answer_id)
    comment = iris.comment(comment_id, PostReference(answer_id, True, 2, question_id), answer)
    return select(
        "?body ?text ?score",
        f"<{question}> so:hasAnswer <{answer}> . <{answer}> so:hasComment <{comment}> . OPTIONAL {{ <{answer}> so:hasBody ?body }} OPTIONAL {{ <{comment}> so:hasText ?text }} OPTIONAL {{ <{comment}> so:score ?score }}",
        (graph,),
        2,
    )


def tag_bundle(graph: str, tag_name: str) -> str:
    """Build a query for a tag's ID, name, wiki, and excerpt."""
    tag = IriPolicy().tag(tag_name)
    return select(
        "?name ?id ?wikiBody ?excerptBody",
        f"<{tag}> so:tagName ?name ; so:tagId ?id . OPTIONAL {{ <{tag}> so:hasTagWiki/so:hasBody ?wikiBody }} OPTIONAL {{ <{tag}> so:hasTagExcerpt/so:hasBody ?excerptBody }}",
        (graph,),
        2,
    )


def creation_date(graph: str, question_id: int) -> str:
    """Build a query for a question's optional creation timestamp."""
    question = IriPolicy().question(question_id)
    return select(
        "?created", f"<{question}> a so:Question . OPTIONAL {{ <{question}> so:creationDate ?created }}", (graph,), 2
    )


def source_counts(graph: str, question_id: int) -> str:
    """Build a query for optional question score and source counts."""
    question = IriPolicy().question(question_id)
    body = f"<{question}> a so:Question . " + " ".join(
        f"OPTIONAL {{ <{question}> so:{predicate} ?{variable} }}"
        for predicate, variable in (
            ("score", "score"),
            ("viewCount", "views"),
            ("answerCount", "answers"),
            ("commentCount", "comments"),
        )
    )
    return select("?score ?views ?answers ?comments", body, (graph,), 2)
