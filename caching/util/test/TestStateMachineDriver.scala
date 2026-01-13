package com.databricks.caching.util

import java.time.Instant

/**
 * Test driver to perform events and advance time in the specified [[StateMachine]] for testing.
 */
class TestStateMachineDriver[EventT, ActionT](stateMachine: StateMachine[EventT, ActionT]) {

  /**
   * Informs the state machine, about the event and the elapsed time. See
   * [[stateMachine.onEvent()]].
   */
  def onEvent(
      tickerTime: TickerTime,
      instant: Instant,
      event: EventT): StateMachineOutput[ActionT] = {
    stateMachine.onEventInternal(tickerTime, instant, event)
  }

  /** Informs the state machine, about elapsed time. See [[stateMachine.onAdvance()]]. */
  def onAdvance(tickerTime: TickerTime, instant: Instant): StateMachineOutput[ActionT] = {
    stateMachine.onAdvanceInternal(tickerTime, instant)
  }

  /** Returns the state machine being tested. */
  def getStateMachine: StateMachine[EventT, ActionT] = stateMachine
}
