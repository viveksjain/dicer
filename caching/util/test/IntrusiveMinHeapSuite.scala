package com.databricks.caching.util

import com.databricks.testing.DatabricksTest
import scala.collection.mutable
import scala.math.Ordering.Long
import scala.util.Random

import org.scalatest.matchers.should.Matchers._

import TestUtils.assertThrow

/** Priority type for the heap under test. */
case class Priority(value: Long) extends AnyVal with Ordered[Priority] {
  override def compare(that: Priority): Int = Long.compare(this.value, that.value)
}

/**
 * Element type for the heap under test. All protected methods are exposed (some with additional
 * internal invariant checks) for testing, as these methods form the type's public surface.
 */
class Element private extends IntrusiveMinHeapElement[Priority] {

  /**
   * Reference to the heap to which this element is attached, or null if the element is detached.
   */
  var testHeap: Heap = _

  /**
   * Override of [[IntrusiveMinHeapElement.attached]] asserting that true is returned by the base
   * implementation iff. the element is attached to a heap.
   */
  def attachedForTest: Boolean = {
    val result = attached
    val expectedResult = testHeap != null
    assert(result == expectedResult)
    result
  }

  def getPriorityForTest: Priority = getPriority

  def setPriorityForTest(priority: Priority): Unit = {
    setPriority(priority)
    forTest.checkInvariants()
  }

  /**
   * Override of [[IntrusiveMinHeapElement.remove]] that verifies the expected behavior of the base
   * implementation, checks invariants, and that removes this element from the "oracle"
   * implementation maintained in [[testHeap]] so that behaviors of subsequent operations can also
   * be validated.
   */
  def removeForTest(): Unit = {
    remove()

    // Remove the element from the test oracle as well.
    testHeap.Oracle.remove(this)
    testHeap = null
    forTest.checkInvariants()
  }
}

object Element {

  /** Returns a new element with `startingPriority` set before returning. */
  def create(startingPriority: Priority): Element = {
    val element: Element = new Element
    element.setPriorityForTest(startingPriority)
    element
  }
}

/**
 * Wrapper around the heap under test that check data invariants after every modification.
 *
 * Compares the behavior of the heap under test with that of a trivial "oracle" that stores all
 * elements in a list that it scans to identify the expected topmost element of the heap.
 */
class Heap {
  val wrappedHeap = new IntrusiveMinHeap[Priority, Element]

  /** The oracle implementation. */
  object Oracle {
    val elements = new mutable.ArrayBuffer[Element]

    /** The oracle's implementation of [[IntrusiveMinHeap.push]]. */
    def push(elem: Element): Unit = {
      require(elem.testHeap == null)
      elements += elem
      elem.testHeap = Heap.this
    }

    /** The oracle's implementation of [[IntrusiveMinHeap.peek]]. */
    def peek: Option[Element] = {
      if (elements.isEmpty) {
        return None
      }
      Some(elements.minBy(_.getPriorityForTest))
    }

    /** The oracle's implementation of [[IntrusiveMinHeap.pop]]. */
    def pop(): Element = {
      require(elements.nonEmpty)
      val top: Option[Element] = peek
      remove(top.get)
      top.get
    }

    /**
     * REQUIRES: elem is in this heap.
     *
     * Removes and detaches the given element from the oracle.
     */
    def remove(elem: Element): Unit = {
      val popped: Element = elements.remove(elements.indexOf(elem))
      popped.testHeap = null // detach
    }
  }

  /**
   * Wrapper around [[IntrusiveMinHeap.push]] that checks invariants and updates the state of the
   * oracle.
   */
  def push(elem: Element): Unit = {
    wrappedHeap.push(elem)
    wrappedHeap.forTest.checkInvariants()
    Oracle.push(elem)
  }

  /**
   * Wrapper around [[IntrusiveMinHeap.size]] that compares the outcome to the one expected by the
   * oracle.
   */
  def size: Int = {
    val result = wrappedHeap.size
    val expectedResult = Oracle.elements.size
    assert(result == expectedResult)
    result
  }

  /**
   * Wrapper around [[IntrusiveMinHeap.isEmpty]] that compares the outcome to the one expected by
   * the oracle.
   */
  def isEmpty: Boolean = {
    val result = wrappedHeap.isEmpty
    val expectedResult = Oracle.elements.isEmpty
    assert(result == expectedResult)
    result
  }

  /**
   * Wrapper around [[IntrusiveMinHeap.nonEmpty]] that compares the outcome to the one expected by
   * the oracle.
   */
  def nonEmpty: Boolean = {
    val result = wrappedHeap.nonEmpty
    val expectedResult = Oracle.elements.nonEmpty
    assert(result == expectedResult)
    result
  }

  /**
   * Wrapper around [[IntrusiveMinHeap.peek]] that compares the outcome to the one expected by the
   * oracle.
   */
  def peek(): Option[Element] = {
    val result = wrappedHeap.peek
    val expectedResult = Oracle.peek
    assert(result == expectedResult)
    result
  }

  /**
   * Wrapper around [[IntrusiveMinHeap.pop]] that checks invariants and compares the outcome to the
   * one expected by the oracle.
   */
  def pop(): Element = {
    val result: Element = wrappedHeap.pop()
    val expectedResult: Element = Oracle.pop()
    assert(result == expectedResult)
    wrappedHeap.forTest.checkInvariants()
    result
  }

  /**
   * Returns copy of the internal heap array. All tests validating the internal layout should be
   * tagged "glass box" as the precise layout of the internal heap array is not part of the contract
   * for the heap.
   */
  def copyContents(): List[Element] = wrappedHeap.forTest.copyContents()
}

class IntrusiveMinHeapSuite extends DatabricksTest {
  test("Push") {
    // Test plan: verify that the expected element is at the heap across three `push` requests.
    val heap = new Heap
    assert(heap.peek().isEmpty)
    val pri4 = Element.create(Priority(4))
    heap.push(pri4)
    assert(heap.peek().contains(pri4))
    val pri2 = Element.create(Priority(2))
    heap.push(pri2)
    assert(heap.peek().contains(pri2))
    val pri3 = Element.create(Priority(3))
    heap.push(pri3)
    assert(heap.peek().contains(pri2))
  }

  test("Push element onto two heaps") {
    // Test plan: verify that an element can only be pushed onto one heap.
    val heap1 = new Heap
    val heap2 = new Heap
    val elem = Element.create(Priority(42))
    heap1.push(elem)
    assert(heap1.size == 1)
    assertThrow[IllegalArgumentException]("Can only push detached element") { heap2.push(elem) }
    assert(heap2.size == 0)
    assert(heap1.peek().contains(elem))
    assert(heap2.peek().isEmpty)
  }

  test("Push element onto same heap twice") {
    // Test plan: verify that an element can only be pushed onto a heap once.
    val heap = new Heap
    val elem = Element.create(Priority(42))
    heap.push(elem)
    assert(heap.size == 1)
    assert(heap.peek().contains(elem))
    assertThrow[IllegalArgumentException]("Can only push detached element") { heap.push(elem) }
    assert(heap.peek().contains(elem))
    assert(heap.size == 1)
  }

  test("Push to top (glass box)") {
    // Test plan: push an element that is expected to sift to the top of the heap. Verify the
    // expected outcome (elements along the path from the bottom of the heap to the top are pushed
    // down).
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 10) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)

    // Push element that should rise to the top. Moved elements are highlighted /**/!
    val elem = Element.create(Priority(-1))
    heap.push(elem)
    assert(
      heap.copyContents() == List(
        List( /**/ elem),
        List(elems( /**/ 0), elems(2)),
        List(elems(3), elems( /**/ 1), elems(5), elems(6)),
        List(elems(7), elems(8), elems(9), elems( /**/ 4))
      ).flatten
    )
  }

  test("Push to mid (glass box)") {
    // Test plan: push an element that is expected to sift to the middle of the heap. Verify the
    // expected outcome (elements along the path from the bottom to the middle of the heap are
    // pushed down).
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 10) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)

    // Push element that should rise to the middle. Moved elements are highlighted /**/!
    val elem = Element.create(Priority(1))
    heap.push(elem)
    assert(
      heap.copyContents() == List(
        List(elems(0)),
        List(elems(1), elems(2)),
        List(elems(3), /**/ elem, elems(5), elems(6)),
        List(elems(7), elems(8), elems(9), elems( /**/ 4))
      ).flatten
    )
  }

  test("Push stays at bottom (glass box)") {
    // Test plan: push an element that is expected to remain at the bottom of the heap. Verify the
    // expected outcome.
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 10) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)

    // Push element that should remain at the bottom of the heap.
    val elem = Element.create(Priority(4))
    heap.push(elem)
    assert(
      heap.copyContents() == List(
        List(elems(0)),
        List(elems(1), elems(2)),
        List(elems(3), elems(4), elems(5), elems(6)),
        List(elems(7), elems(8), elems(9), /**/ elem)
      ).flatten
    )
  }

  test("Pop") {
    // Test plan: populate the heap then repeatedly pop. Verify that the expected elements are at
    // the top of the heap across these pops.
    val heap = new Heap
    val pri1 = Element.create(Priority(1))
    val pri2 = Element.create(Priority(2))
    val pri3 = Element.create(Priority(3))
    heap.push(pri1)
    heap.push(pri2)
    heap.push(pri3)

    // Start popping!
    assert(heap.size == 3)
    assert(heap.peek().contains(pri1))
    assert(heap.pop() == pri1)
    assert(heap.size == 2)
    assert(heap.peek().contains(pri2))
    assert(heap.pop() == pri2)
    assert(heap.size == 1)
    assert(heap.peek().contains(pri3))
    assert(heap.pop() == pri3)
    assert(heap.size == 0)
    assert(heap.peek().isEmpty)
  }

  test("Pop from empty heap") {
    // Test plan: verify that popping from an empty heap (initial, or after removing all elements)
    // returns false.
    val heap = new Heap
    assertThrow[IllegalArgumentException]("Can only pop an element from a non-empty heap") {
      heap.pop()
    } // nothing to pop initially
    val elem = Element.create(Priority(42))
    heap.push(elem)
    elem.removeForTest()
    assertThrow[IllegalArgumentException]("Can only pop an element from a non-empty heap") {
      heap.pop()
    } // nothing left to pop
  }

  test("Pop, replacement sifts to bottom with sibling (glass box)") {
    // Test plan: pop an element such that the replacement, taken from the end of the heap, sifts
    // back down to the bottom, where the leaf has a sibling.
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 10) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)
    assert(heap.pop() == elems(0))

    // The replacement element (elems(9)) should sift to the bottom of the heap. Moved elements are
    // highlighted /**/!
    assert(
      heap.copyContents() == List(
        List( /**/ elems(1)),
        List( /**/ elems(3), elems(2)),
        List( /**/ elems(7), elems(4), elems(5), elems(6)),
        List( /**/ elems(9), elems(8))
      ).flatten
    )
  }

  test("Pop, replacement sifts to bottom with no sibling (glass box)") {
    // Test plan: pop an element such that the replacement, taken from the end of the heap, sifts
    // back down to the bottom, where the leaf has no sibling.
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 9) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)
    assert(heap.pop() == elems(0))

    // The replacement element (elems(8)) should sift to the bottom of the heap. Moved elements are
    // highlighted /**/!
    assert(
      heap.copyContents() == List(
        List( /**/ elems(1)),
        List( /**/ elems(3), elems(2)),
        List( /**/ elems(7), elems(4), elems(5), elems(6)),
        List( /**/ elems(8))
      ).flatten
    )
  }

  /**
   * Creates a heap with `n` elements, removes an element at the given `index`, and finally pops all
   * elements in the heap. Expects to observe all pushed elements (in priority order) except for the
   * element that was removed. Factored as a separate method so that we can test removal of elements
   * from different positions in the heap.
   *
   * @param n     number of elements in the heap
   * @param index index of element to remove
   */
  private def testRemove(n: Int, index: Int): Unit = {
    val heap = new Heap
    val n = 10
    val elems: IndexedSeq[Element] = for (i <- 0 until n) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)

    // Remove the element at `index` from the heap. Verify that remaining elements are yielded in
    // order.
    elems(index).removeForTest()
    assert(heap.size == n - 1)
    var expectedIndex = 0
    do {
      if (expectedIndex == index) {
        // Expect to skip the removed element.
        expectedIndex += 1
      }
      val top = heap.peek()
      assert(top.contains(elems(expectedIndex)))
      heap.pop()
      expectedIndex += 1
    } while (!heap.isEmpty)
  }

  test("Remove from top") {
    // Test plan: remove an element from the top of the heap. Equivalent to pop().
    testRemove(10, 0)
  }

  test("Remove from end") {
    // Test plan: remove an element from the end of the heap.
    testRemove(10, 9)
  }

  test("Remove from middle") {
    // Test plan: remove an element from the middle of the heap.
    testRemove(10, 4)
  }

  test("Remove from bottom") {
    // Test plan: remove an element from the bottom of the heap.
    testRemove(10, 8)
  }

  test("Remove detached element") {
    // Test plan: remove an element that is not in any heap. Should be a no-op.
    val detachedElem = Element.create(Priority(42))
    assert(!detachedElem.attachedForTest)
    assertThrow[IllegalArgumentException]("Can only remove attached element") {
      detachedElem.removeForTest()
    }
  }

  test("Remove, replacement element sifts up (glass box)") {
    // Test plan: construct a heap such the replacement element (taken from the end of the heap)
    // for a removed element needs to sift up.
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 7) yield Element.create(Priority(i))
    val rawContents = List(
      List(elems(0)),
      List(elems(4), elems(1)),
      List(elems(5), elems(6), elems(2), elems(3))
    ).flatten
    for (elem <- rawContents) heap.push(elem)
    assert(heap.copyContents() == rawContents)

    // Remove elems(6), which will be replaced by elems(3) which then needs to sift up. Verify
    // expected contents after the removal (moved elements are highlighted /**/!)
    elems(6).removeForTest()
    assert(
      heap.copyContents() == List(
        List(elems(0)),
        List( /**/ elems(3), elems(1)),
        List(elems(5), /**/ elems(4), elems(2))
      ).flatten
    )
  }

  test("Remove, replacement element sifts down (glass box)") {
    // Test plan: construct a heap such the replacement element (taken from the end of the heap)
    // for a removed element needs to sift down.
    val heap = new Heap
    val elems = for (i <- 0 until 7) yield Element.create(Priority(i))
    val rawContents = List(
      List(elems(0)),
      List(elems(4), elems(1)),
      List(elems(5), elems(6), elems(2), elems(3))
    ).flatten
    for (elem <- rawContents) heap.push(elem)
    assert(heap.copyContents() == rawContents)

    // Remove elems(1), which will be replaced by elems(3) which then needs to sift down. Verify
    // expected contents after the removal (moved elements are highlighted /**/!)
    elems(1).removeForTest()
    assert(
      heap.copyContents() == List(
        List(elems(0)),
        List(elems(4), /**/ elems(2)),
        List(elems(5), elems(6), /**/ elems(3))
      ).flatten
    )
  }

  test("SetPriority of detached element") {
    // Test plan: update the priority of an element before it has been pushed onto a heap. Verify
    // that when pushed, its position reflects the updated priority.
    val elem = Element.create(Priority(1))
    assert(elem.getPriorityForTest == Priority(1))
    elem.setPriorityForTest(Priority(42))
    assert(elem.getPriorityForTest == Priority(42))
    val heap = new Heap
    heap.push(Element.create(Priority(2)))
    heap.push(Element.create(Priority(47)))
    heap.push(elem)

    // The updated element should be second off the heap.
    heap.pop()
    assert(heap.peek().contains(elem))
  }

  test("SetPriority of top, sifts down") {
    // Test plan: update the priority of an element that is currently on the top of the heap such
    // that it sifts down.
    val heap = new Heap
    val elems: IndexedSeq[Element] =
      for (i <- 0 until 10) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)
    assert(heap.peek().contains(elems.head))
    elems.head.setPriorityForTest(Priority(10))
    assert(!heap.peek().contains(elems.head))
  }

  test("SetPriority of top, remains top") {
    // Test plan: update the priority of an element that is currently on the top of the heap such
    // that its relative priority does not change.
    val heap = new Heap
    val elems: IndexedSeq[Element] = for (i <- 0 until 10) yield Element.create(Priority(i))
    for (elem <- elems) heap.push(elem)
    assert(heap.peek().contains(elems.head))
    elems.head.setPriorityForTest(Priority(-1))
    assert(heap.peek().contains(elems.head))
  }

  test("Push-order tie-breaker") {
    // Test plan: push multiple elements with the same priority, interleaved with other elements.
    // Verify that the elements with same priority are popped in the order they were pushed.
    val heap = new Heap
    val sharedPri = Priority(42)
    val elems: IndexedSeq[Element] = for (_ <- 0 until 10) yield Element.create(sharedPri)
    val random = new Random
    for (elem <- elems) {
      heap.push(elem)

      // Interleave some other element. It's ok if this element also has priority 42.
      heap.push(Element.create(Priority(random.nextInt(100))))
    }
    // Verify that all elements in `elems` are popped in order.
    var i = 0
    while (i < elems.size) {
      val top = heap.pop()
      if (top eq elems(i)) {
        i += 1
      }
    }
  }

  test("Push after pop (glass box)") {
    // Test plan: verify that an element can be pushed back onto a heap after it is popped.
    val heap1 = new Heap
    val heap2 = new Heap
    val elem = Element.create(Priority(42))
    heap1.push(elem)
    assert(heap1.copyContents() == List(elem))

    // Pop and then push onto the same heap.
    assert(heap1.pop() == elem)
    heap1.push(elem)
    assert(heap1.copyContents() == List(elem))

    // Pop and then push onto a different heap.
    assert(heap1.pop() == elem)
    heap2.push(elem)
    assert(heap2.copyContents() == List(elem))
  }

  test("Randomized") {
    // Test plan: perform random actions against heaps and elements. Relies on the test `Heap`
    // wrapper to verify expected behavior. In each trial, the test creates a few empty heaps, and
    // then runs multiple "steps". During each step, an action is chosen as random (e.g., "push" or
    // "pop") and applied to a random heap. Actions are weighted at each step so that they are not
    // equally likely to be selected. For example, impossible actions (like "pop" when all heaps are
    // empty) have zero weight. Once the prescribed number of steps have been taken, the queues
    // are drained as a final test.
    //
    // The goal of these tests is to ensure that various heap layouts and action sequences are
    // exercised, beyond those explicitly tested in this suite.
    val random = new Random(42)

    // Chooses a random priority for an element.
    def chooseRandomPriority(): Priority = Priority(random.nextInt(40))

    // Chooses a random element of `seq`.
    def chooseRandom[T](seq: IndexedSeq[T]): T = seq(random.nextInt(seq.size))

    // Removes the element at the given `index` from `array`. REQUIRES: `array` is non-empty.
    def remove[T](array: mutable.ResizableArray[T], index: Int): Unit = {
      if (index != array.size - 1) {
        array(index) = array.last
      }
      array.reduceToSize(array.size - 1)
    }

    // Removes and returns a random element from `array`. REQUIRES: `array` is non-empty.
    def removeRandom[T](array: mutable.ResizableArray[T]): T = {
      val index = random.nextInt(array.size)
      val result = array(index)
      remove(array, index)
      result
    }

    for (_ <- 0 until 100) { // run multiple trials
      // Heaps under test. Heaps are selected at random at each step in the test.
      val heaps: IndexedSeq[Heap] = for (_ <- 0 until 3) yield new Heap

      // Tracks elements that are not attached to any heap.
      val detached = new mutable.ArrayBuffer[Element]

      // Tracks elements that are attached to a heap.
      val attached = new mutable.ArrayBuffer[Element]

      // Possible actions that may be taken during a step in the randomized test, consisting of a
      // weight which is proportional to the likelihood the action will be chosen, and the function
      // that can be invoked to actuate it.
      case class Action(description: String, weight: Int, work: () => Unit)

      // Generators set weights for each action based on the current test context. Actions that are
      // invalid (e.g., "push detached element" when there are no detached elements) have weight 0.
      // The weights are otherwise not very scientific: actions that are expected to have no side
      // effects have weight 1 and side-effecting actions have weight 5.

      // Minimum desired number of attached heap elements. When below this threshold, actions that
      // decrease the number of attached elements ("pop" or "remove") are given zero weight.
      val minAttachedSize = 100

      // Maximum desired number of attached heap elements. When above this threshold, actions that
      // increase the number of attached elements ("push") are given zero weight.
      val maxAttachedSize = 200
      val actionGenerators = List[() => Action](
        () =>
          Action(
            "push new element",
            if (attached.size < maxAttachedSize) 5 else 0,
            () => {
              val heap = chooseRandom(heaps)
              val element = Element.create(chooseRandomPriority())
              attached += element
              heap.push(element)
            }
          ),
        () =>
          Action(
            "push detached element",
            if (detached.nonEmpty && attached.size < maxAttachedSize) 5 else 0,
            () => {
              val heap = chooseRandom(heaps)
              val element = removeRandom(detached)
              attached += element
              heap.push(element)
            }
          ),
        () =>
          Action(
            "push attached element (no-op)",
            if (attached.nonEmpty) 1 else 0,
            () => {
              val heap = chooseRandom(heaps)
              val element = chooseRandom(attached)
              assertThrow[IllegalArgumentException]("Can only push detached element") {
                heap.push(element)
              }
            }
          ),
        () =>
          Action(
            "pop",
            if (heaps.exists(!_.isEmpty) && attached.size > minAttachedSize) 5 else 0,
            () => {
              val heap: Heap = chooseRandom(heaps.filter(!_.isEmpty))
              val element = heap.peek().get
              remove(attached, attached.indexOf(element))
              detached += element
              assert(heap.pop() == element)
            }
          ),
        () =>
          Action(
            "remove attached element",
            if (attached.nonEmpty && attached.size > minAttachedSize) 5 else 0,
            () => {
              val element = removeRandom(attached)
              detached += element
              element.removeForTest()
            }
          ),
        () =>
          Action(
            "remove detached element (no-op)",
            if (detached.nonEmpty) 1 else 0,
            () => {
              val element = detached(random.nextInt(detached.size))
              assertThrow[IllegalArgumentException]("Can only remove attached element") {
                element.removeForTest()
              }
            }
          ),
        () =>
          Action(
            "update attached element",
            if (attached.nonEmpty) 5 else 0,
            () => {
              val element = chooseRandom(attached)
              element.setPriorityForTest(chooseRandomPriority())
            }
          ),
        () =>
          Action(
            "update detached element",
            if (detached.nonEmpty) 5 else 0,
            () => {
              val element = chooseRandom(detached)
              element.setPriorityForTest(chooseRandomPriority())
            }
          ),
        () =>
          Action("peek", 5, () => {
            val heap = chooseRandom(heaps)
            heap.peek()
          })
      )

      // Keep track of actions taken by description
      val actionCounts = new mutable.HashMap[String, Int]
      for (_ <- 0 until 5000) { // multiple actions per trial
        // Collect all actions at the current step. We call the action generators at each step
        // because the weights for actions may change based on the current state of the heaps under
        // test. For example, the action that removes a random element has weight zero when there
        // are no attached elements (`attachedElement.isEmpty`).
        val actions: List[Action] = for (generator <- actionGenerators) yield generator()

        // Choose an action at random based on weights. For illustrative purposes, consider actions
        // with weights {1, 2, 0, 3}. We first convert these weights into "cumulative" weights
        // {1, 3, 3, 6} where each accumulates the weight of action at the given index and the
        // weights of all previous actions.
        val cumulativeWeights: List[Int] = actions.map(_.weight).scanLeft(0)(_ + _).drop(1)
        assert(cumulativeWeights.size == actions.size)

        // Now pick a random number in [0, totalWeight), or [0, 6) in our running example.
        val totalWeight: Int = cumulativeWeights.last
        val roll: Int = random.nextInt(totalWeight)

        // Choose the first action whose cumulative weight is greater than the rolled value. In
        // our running example, roll=0 maps to the first action (1/6 chance), roll=1 and roll=2 map
        // to the second action (2/6 chance), nothing maps to the third action, and the remaining
        // rolls map to the fourth action (3/6 chance). As you can see, the action weights determine
        // the proportional likelihood of each action being selected based on the roll.
        val actionIndex: Int = cumulativeWeights.indexWhere(roll < _)
        val action: Action = actions(actionIndex)

        // Perform action.
        action.work()
        val count = actionCounts.getOrElse(action.description, 0) + 1
        actionCounts.put(action.description, count)
      }
      assert(
        actionGenerators.size == actionCounts.size,
        "expect all actions to be explored: " + actionCounts
      )
      logger.info("random action counts: " + actionCounts)

      // Drain all heaps as an additional sanity check.
      for (heap <- heaps) {
        while (heap.nonEmpty) {
          heap.pop()
        }
      }
    }
  }

  test("Element.toString") {
    // Test plan: Verify that the `toString` method of an `IntrusiveMinHeapElement` returns the
    // expected representation.
    val elem = Element.create(Priority(42))
    assert(elem.toString == "detached,pri=Priority(42)")
    val heap = new Heap
    heap.push(elem)
    assert(elem.toString == "pri=Priority(42),pushOrder=0,index=0")
  }
}
