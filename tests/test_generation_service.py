import importlib

import services.generation_service as generation_service


def _drain_generation_queue() -> None:
    try:
        while True:
            generation_service._GENERATION_QUEUE.get_nowait()  # noqa: SLF001
            generation_service._GENERATION_QUEUE.task_done()  # noqa: SLF001
    except Exception:
        return


def _reset_generation_state() -> None:
    _drain_generation_queue()
    generation_service._GENERATION_ENQUEUED.clear()  # noqa: SLF001
    generation_service._GENERATION_SEQ = 0  # noqa: SLF001


def test_generation_queue_priority_orders_manual_before_auto():
    importlib.reload(generation_service)
    _reset_generation_state()

    assert generation_service.enqueue_actor_generation_core(
        actor_id='actor-auto',
        deps={'trigger_type': 'auto_refresh', 'priority': 2},
    )
    assert generation_service.enqueue_actor_generation_core(
        actor_id='actor-manual',
        deps={'trigger_type': 'manual_refresh', 'priority': 0},
    )

    first = generation_service._GENERATION_QUEUE.get_nowait()  # noqa: SLF001
    second = generation_service._GENERATION_QUEUE.get_nowait()  # noqa: SLF001
    generation_service._GENERATION_QUEUE.task_done()  # noqa: SLF001
    generation_service._GENERATION_QUEUE.task_done()  # noqa: SLF001

    assert first[2] == 'actor-manual'
    assert second[2] == 'actor-auto'


def test_generation_enqueue_dedupes_per_actor():
    importlib.reload(generation_service)
    _reset_generation_state()

    first = generation_service.enqueue_actor_generation_core(
        actor_id='actor-1',
        deps={'trigger_type': 'manual_refresh'},
    )
    second = generation_service.enqueue_actor_generation_core(
        actor_id='actor-1',
        deps={'trigger_type': 'manual_refresh'},
    )

    assert first is True
    assert second is False

