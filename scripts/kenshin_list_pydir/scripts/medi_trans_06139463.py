def collect_namespaces(xml_bytes: bytes) -> List[Tuple[str, str]]:
    """Collect (prefix, uri) namespace declarations from XML bytes."""
    out: List[Tuple[str, str]] = []
    # iterparse yields ('start-ns', (prefix, uri)) but type stubs can vary (pyright/pylance)
    for _event, ns in ET.iterparse(io.BytesIO(xml_bytes), events=("start-ns",)):
        # ns may be typed as unknown/Element in some stubs, so normalize to str explicitly
        try:
            prefix, uri = ns  # type: ignore[misc]
        except Exception:
            # safety fallback; skip malformed namespace events
            continue

        p = "" if prefix is None else str(prefix)
        u = "" if uri is None else str(uri)
        out.append((p, u))
    return out