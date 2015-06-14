package org.pasteur

import java.util
import java.util.UUID
import java.util.UUID.randomUUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

import org.pasteur.HyParView._
import org.pasteur.Underlay.{CnxnEstablished, CnxnLost, ConnectionEvent}
import rx.Observable.OnSubscribe
import rx.subjects.PublishSubject
import rx.subscriptions.CompositeSubscription
import rx.{Observable, Observer, Subscriber}

object HyParView {

    val FORWARD_JOIN_TTL = 5
    val SHUFFLE_TTL = 5
    val PASSIVE_RANDOM_WALK_LENGTH = 3
    val ACTIVE_RANDOM_WALK_LENGTH = 3
    val SHUFFLE_NUM_ACTIVE = 3
    val SHUFFLE_NUM_PASSIVE = 3

    abstract class Message(val id: UUID, val source: Int, val dest: Int) {
        if (source == dest) {
            throw new IllegalArgumentException(
                "Source and destination must not be equal")
        }
        if (id == null)
            throw new NullPointerException(
                s"Nodes with null id are not allowed")
    }

    case class Join(override val id: UUID, override val source: Int,
                    override val dest: Int) extends Message(id, source, dest)

    case class Disconnect(override val id: UUID, override val source: Int,
                      override val dest: Int) extends Message(id, source, dest)

    case class ForwardJoin(override val id: UUID, override val source: Int,
                           override val dest: Int, joinedNode: Int, ttl: Int)
        extends Message(id, source, dest)

    case class ShuffleRequest(override val id: UUID, override val source: Int,
                              override val dest: Int, ttl: Int,
                              active: Set[Int], passive: Set[Int])
        extends Message(id, source, dest)

    case class ShuffleReply(override val id: UUID, override val source: Int,
                            override val dest: Int, active: Set[Int])
        extends Message(id, source, dest)

}

object Underlay {
    abstract class ConnectionEvent(val nodeId: Int)
    case class CnxnEstablished(override val nodeId: Int)
        extends ConnectionEvent(nodeId)
    case class CnxnLost(override val nodeId: Int)
        extends ConnectionEvent(nodeId)
}

trait Underlay {

    /** Emits every message received by this node */
    val receiver: Observable[HyParView.Message] = Observable.create(onConnect)

    implicit val ec: ExecutionContext

    /** Setup the inbound connection to start receiving messages from other
      * members of the cluster.
      */
    def onConnect: OnSubscribe[HyParView.Message]

    /** Child classes should publish connection events here */
    protected[Underlay] val cnxnEvents =
        PublishSubject.create[ConnectionEvent]()

    def connectionEvents
    : Observable[ConnectionEvent] = cnxnEvents.asObservable()

    def origin: Int

    final def send(m: HyParView.Message): Unit = {
        doSend(m).onFailure { case t: Throwable =>
            cnxnEvents.onNext(Underlay.CnxnLost(m.dest))
        }
    }

    def doSend(m: HyParView.Message): Future[HyParView.Message]

    def testConnectionTo(n: Int): Future[Int]

}

class TCPUnderlay(val origin: Int)
                 (implicit val ec: ExecutionContext) extends Underlay {

    override def connectionEvents = cnxnEvents.asObservable()

    override def doSend(m: HyParView.Message)
    : Future[HyParView.Message] = {
        // TODO
        Future.successful(m)
    }

    override def onConnect = new OnSubscribe[HyParView.Message] {
        // Open TCP socket, etc.
        override def call(s: Subscriber[_ >: Message]): Unit = ???
    }

    override def testConnectionTo(n: Int): Future[Int] = ???
}

/** A Node's partial view of the overlay network for a given node. */
class Overlay(val myId: Int, val fanout: Int) {

    if (myId <= 0) {
        throw new IllegalArgumentException(
            s"Node ids can only be positive integers (given: $myId)")
    }

    private val rand = new java.util.Random

    val maxActiveSize = fanout + 1
    val maxPassiveSize = fanout * 5

    // TODO: these could very well be Sets, just setting a map in case metadata
    // makes sense, probably not needed
    private val active: util.Map[Int, Int] =
        new ConcurrentHashMap[Int, Int](maxActiveSize)
    private val passive: util.Map[Int, Int] =
        new ConcurrentHashMap[Int, Int](maxPassiveSize)

    /**
     * Ensures that the passive view has at most n elements.  If the size
     * exceeds n, it'll drop first elements contained in the preferDrop set,
     * then random elements.
     *
     * Assumes that neither the passive, nor the preferDrop sets contain the
     * local node id.
     */
    private def ensurePassiveSize(n: Int, preferDrop: Set[Int]): Unit = {
        preferDrop.take(passive.size - n).foreach(passive.remove)
        while (passive.size > n) {
            passive remove passive.keys.toSeq.get(rand.nextInt(passive.size - 1))
        }
    }

    /** Randomly choses n elements from the given map */
    private def chooseFrom(which: util.Map[Int, Int])(n: Int)
    : Set[Int] = Random.shuffle(which.keys).take(n).toSet

    @inline def passiveView: Set[Int] = passive.keySet().toSet
    @inline def activeView: Set[Int] = active.keySet().toSet
    @inline def isActiveFull: Boolean = active.size >= maxActiveSize

    /**
     * Adds nodeIds to the passive view.  If the size of the view exceeds the
     * maximum, it'll drop first elements contained in dropFrom, then random
     * elements.
     */
    def addToPassive(nodeIds: Set[Int], dropFrom: Set[Int] = Set.empty)
    : Unit = {
        val toAdd = nodeIds filterNot { id =>
            myId == id || active.contains(id)
        }
        if (toAdd.nonEmpty) {
            ensurePassiveSize(maxPassiveSize - toAdd.size, dropFrom)
            toAdd foreach { nodeId =>
                passive put (nodeId, nodeId)
            }
        }
    }

    /** Chose n random elements from the Active view. */
    def chooseFromActive(n: Int) = chooseFrom(active)(n)

    /** Chose n random elements from the Passive view. */
    def chooseFromPassive(n: Int) = chooseFrom(passive)(n)

    /** Adds the given node to the Active view.  If the active view doesn't
      * admit more elements, then it will drop randomly chosen nodes from the
      * active view to the passive view.
      *
      * @return the node dropped to the passive view if any, or null otherwise
      */
    def addToActive(nodeId: Int): Option[Int] = {
        var dropped: Option[Int] = None
        if (myId != nodeId && !active.contains(nodeId)) {
            if (isActiveFull) {
                dropped = chooseRandomActive() flatMap downgrade
            }
            active put (nodeId, nodeId)
        }
        dropped
    }

    /** Removes the given node from the Active view, moving it to the passive
      * view.
      */
    def downgrade(nodeId: Int): Option[Int] = {
        val dropped = Option(active remove nodeId)
        dropped foreach { id => addToPassive(Set(id)) }
        dropped
    }

    /** Takes a node from the passive into the active view */
    def upgrade(nodeId: Int): Unit = {
        val removed = passive remove nodeId
        if (null != removed) {
            addToActive(nodeId)
        }
    }

    /** Applies the given function to every node of the active view. */
    def applyToActive[T](f: Int => T): Unit = active.values foreach f

    def activeSize: Int = active.size()
    def passiveSize: Int = passive.size()

    /** Choses a random node from the Active view that is not the given one */
    def chooseRandomActive(not: Option[Int] = None): Option[Int] = {
        val nodes = active.keySet().toSet
        val eligible = if (not.isEmpty) nodes else nodes - not.get
        if (eligible.isEmpty) {
            None
        } else {
            Some(Random.shuffle(eligible).head)
        }
    }
}

/**
 *
 * @param nodeId       my node id
 * @param contactNode  the node to use when joining the overlay
 * @param overlay      the overlay network formed as result of gossiping
 * @param underlay     the underlay network used to gossip
 */
class HyParView(val nodeId: Int,
                val contactNode: Int,
                val overlay: Overlay,
                val underlay: Underlay)
               (implicit val ec: ExecutionContext) {

    import Underlay.ConnectionEvent

    private val subscriptions = new CompositeSubscription

    // Notified when any connection to a Node is established or broken
    private val cnxnEventHandler = new Observer[ConnectionEvent] {
        override def onCompleted(): Unit = ???
        override def onError(e: Throwable): Unit = ???
        override def onNext(e: ConnectionEvent): Unit = e match {
            case CnxnEstablished(id) => // TODO: what?
            case CnxnLost(id) => swapIfActive(id)
        }
    }

    subscriptions.add(underlay.connectionEvents.subscribe(cnxnEventHandler))
    subscriptions.add(underlay.receiver.subscribe (
        new Observer[Message] {
            override def onCompleted(): Unit = ???
            override def onError(e: Throwable): Unit = ???
            override def onNext(m: Message): Unit = receive(m)
        })
    )

    def initialize(): Unit = send(new Join(randomUUID(), nodeId, contactNode))

    def close(): Unit = subscriptions.unsubscribe()

    /** Send the message to the given node, if the connection is broken it'll
      * swap the failed node with one from the passive view that is verified
      * to be reachable.
      */
    def send(msg: Message): Unit = underlay.send(msg)

    /** Takes a node from the active view which connection is verified broken,
      * and swaps it for a reachable node in the passive view.
      */
    private def swapIfActive(nodeId: Int) = {
        val candidateForUpgrade = overlay.chooseFromPassive(1).headOption
        overlay.downgrade(nodeId) flatMap { _ =>
            candidateForUpgrade
        } foreach { candidate =>
            underlay.testConnectionTo(candidate)
                    .foreach { overlay.upgrade }
        }
    }

    def receive(msg: Message): Unit = msg match {

        case Join(msgId, source, dest) if source != nodeId =>
            val droppedNodeId = overlay addToActive source
            overlay applyToActive { node =>
                if (node != source) {
                    send(ForwardJoin(randomUUID(), nodeId, node, source,
                                     FORWARD_JOIN_TTL))
                }
            }
            droppedNodeId foreach { id =>
                send(Disconnect(UUID.randomUUID(), nodeId, id))
            }

        case Join(msgId, source, dest) => // Ignore

        case s: ShuffleRequest =>
            val sendToPeer = if (s.ttl > 0 && overlay.activeSize > 1) {
                                 forwardShuffle(s)
                                 Set.empty[Int]
                             } else {
                                 acceptShuffle(s)
                             }
            overlay.addToPassive(s.active - nodeId, sendToPeer)

        case s: ShuffleReply =>
            overlay.addToPassive(s.active - nodeId, s.active)

        case ForwardJoin(msgId, source, dest, newNode, ttl) =>
            if (ttl == 0 || overlay.activeSize == 1) {
                overlay addToActive newNode
            } else {
                if (ttl == PASSIVE_RANDOM_WALK_LENGTH) {
                    overlay addToPassive Set(newNode)
                }

                overlay.chooseRandomActive(Some(source)) foreach { newDst =>
                    send(ForwardJoin(msgId, nodeId, newDst, newNode, ttl - 1))
                }
            }

        case Disconnect(msgId, source, _) =>
            overlay downgrade source
    }

    def forwardShuffle(req: ShuffleRequest): Unit = {
        // TODO: alter the source? or keep the original?
        overlay chooseRandomActive Some(req.source) foreach { dst =>
            send ( ShuffleRequest(randomUUID(), nodeId, dst,
                                  req.ttl - 1, req.active, req.passive)
            )
        }
    }

    def acceptShuffle(req: ShuffleRequest): Set[Int] = {
        val myActiveSample = overlay.chooseFromActive(req.active.size)
        send (
            ShuffleReply(randomUUID(), nodeId, req.source, myActiveSample)
        )
        myActiveSample
    }

    def suffleToRandomPeer(): Unit = {
        overlay.chooseRandomActive() foreach { dst =>
            send(ShuffleRequest(randomUUID(), nodeId, dst, SHUFFLE_TTL,
                                overlay chooseFromActive SHUFFLE_NUM_ACTIVE,
                                overlay chooseFromPassive SHUFFLE_NUM_ACTIVE))
        }
    }

}
