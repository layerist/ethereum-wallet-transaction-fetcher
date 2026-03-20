async def worker(name, session, state, queue, results):
    while True:
        try:
            address, depth = await queue.get()
        except asyncio.CancelledError:
            break

        try:
            async with state.lock:
                if depth <= 0 or address in state.visited:
                    continue
                state.visited.add(address)

            logger.info("[W%s] %s | depth=%s", name, address, depth)

            txs = await fetch_transactions(session, state, address)

            for tx in txs:
                tx_hash = tx.get("hash")
                if not tx_hash:
                    continue

                async with state.lock:
                    if tx_hash in state.seen_hashes:
                        continue
                    state.seen_hashes.add(tx_hash)

                from_addr = checksum(tx.get("from", ""))
                to_addr = checksum(tx.get("to", ""))

                if not from_addr or not to_addr:
                    continue

                results[tx_hash] = {
                    "hash": tx_hash,
                    "from": from_addr,
                    "to": to_addr,
                    "value_eth": wei_to_eth(tx.get("value", 0)),
                    "timestamp": int(tx.get("timeStamp", 0)),
                }

                if depth > 1:
                    for addr in (from_addr, to_addr):
                        should_enqueue = False

                        async with state.lock:
                            if (
                                addr not in state.visited
                                and addr not in state.enqueued
                            ):
                                state.enqueued.add(addr)
                                should_enqueue = True

                        if should_enqueue:
                            await queue.put((addr, depth - 1))

        finally:
            queue.task_done()
