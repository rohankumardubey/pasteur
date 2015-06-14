package org.pasteur

import java.util.Random
import java.util.UUID.randomUUID

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.pasteur.HyParView._
import org.scalatest.{FeatureSpec, ShouldMatchers}
import rx.Observable.OnSubscribe
import rx.Subscriber
import rx.observers.TestObserver
import rx.subjects.PublishSubject

class PasteurTest extends FeatureSpec with ShouldMatchers {

    val rand = new Random
    val fanout = 10

    feature("Overlay management") {

        scenario("Max view sizes") {
            val o = new Overlay(rand.nextInt(1000), rand.nextInt(1000))
            o.maxActiveSize shouldBe (o.fanout + 1)
            o.maxPassiveSize shouldBe (o.fanout * 5)
        }

        scenario("Node ids are a positive int") {
            intercept[IllegalArgumentException] {
                val n = rand.nextInt
                new Overlay(if (n > 0) n * -1 else n, fanout)
            }
        }

        scenario("Empty overlay") {
            val o = new Overlay(100, fanout)
            o.activeSize shouldBe 0
            o.myId shouldBe 100
            o.chooseFromActive(rand.nextInt()) shouldBe empty
            o.chooseFromPassive(rand.nextInt()) shouldBe empty
            o.removeFromActive(rand.nextInt()) shouldBe None
            o.isActiveFull shouldBe false
            o.activeView shouldBe empty
            o.passiveView shouldBe empty
        }

        scenario("Adding elements to the active view") {
            val o = new Overlay(1000, fanout)
            val newActive = 1 to o.maxActiveSize

            // Fill the active view
            newActive foreach o.addToActive
            o.isActiveFull shouldBe true
            o.activeSize shouldBe o.maxActiveSize
            o.activeView should contain theSameElementsAs newActive
            o.passiveView shouldBe empty

            // Add another one that requires dropping to passive view
            o.addToActive(o.maxActiveSize + 1).get shouldBe <= (fanout + 1)
            o.activeSize shouldBe o.maxActiveSize
            o.activeView should contain (o.maxActiveSize + 1)
            o.passiveView should have size 1

            o.activeView ++ o.passiveView should contain
                                    theSameElementsAs (1 to (fanout+1))
        }

        scenario("A node can't add itself to the active or passive view") {
            val o = new Overlay(Math.abs(rand.nextInt()), 10)
            o.addToActive(o.myId)
            o.activeView shouldNot contain (o.myId)

            o.addToPassive(Set(o.myId))
            o.passiveView shouldNot contain (o.myId)
        }
        
        scenario("Adding a duplicate node to either view has no effect") {
            val o = new Overlay(1, 10)
            val newId = rand.nextInt(1000) + 1

            o.addToActive(newId)
            o.activeView should contain (newId)
            o.activeSize shouldBe 1

            o.addToActive(newId)
            o.activeView should contain (newId)
            o.activeSize shouldBe 1
        }

        scenario("Adding an active node to the passive view has no effect") {
            val o = new Overlay(1, 10)
            val newId = rand.nextInt(1000) + 1
            o.addToActive(newId)
            o.addToPassive(Set(newId))
            o.activeView should contain only newId
            o.passiveView shouldBe empty
        }

        scenario("Adding elements to the passive view") {
            val o = new Overlay(10000, fanout)
            val newPassive = (1 to o.maxPassiveSize).toSet

            // Fill the passive view
            o addToPassive newPassive
            o.passiveSize shouldBe newPassive.size
            o.passiveSize shouldBe o.maxPassiveSize
            o.chooseFromPassive(o.maxPassiveSize) shouldBe newPassive

            // Add more passive nodes, specifying what elements to be dropped
            val toDrop = o.passiveView take 10
            val firstIdx = o.maxPassiveSize + 1
            val morePassive = (firstIdx until firstIdx + 10).toSet
            morePassive should have size toDrop.size
            o.addToPassive(morePassive, toDrop)
            toDrop filter o.passiveView.contains shouldBe empty

            val expectedPassive = newPassive -- toDrop ++ morePassive
            o.passiveView should have size o.maxPassiveSize
            o.passiveView should have size expectedPassive.size
            o.passiveView should contain theSameElementsAs expectedPassive

            // Add more passive nodes, without specifying what elements to drop
            o addToPassive toDrop
            o.passiveView should have size o.maxPassiveSize
            toDrop filterNot o.passiveView.contains shouldBe empty
        }
    }

    class MockUnderlay(nodeId: Int) extends Underlay {
        val out = new TestObserver[Message]()
        val in = PublishSubject.create[Message]
        override def origin: Int = nodeId
        override protected def send(m: Message): Unit = out onNext m
        override def onConnect = new OnSubscribe[HyParView.Message] {
            override def call(s: Subscriber[_ >: Message]): Unit = {
                in subscribe s
            }
        }
    }

    class Context {
        val me = 1
        val other = 2
        val ov = new Overlay(me, fanout)
        private val und = new MockUnderlay(me)

        val hpv = new HyParView(me, other, ov, und)

        def out: TestObserver[Message] = und.out
        def in = und.in

        def assertNothingEmitted() = {
            out.getOnNextEvents shouldBe empty
            out.getOnCompletedEvents shouldBe empty
            out.getOnErrorEvents shouldBe empty
        }
    }

    feature("Message handling") {

        scenario("Messages can't be built with the same source and dest") {
            intercept[IllegalArgumentException] {
                new Join(randomUUID(), 1, 1)
            }
        }

        scenario("Messages can't be built with null id") {
            intercept[NullPointerException] {
                new Join(null, 1, 2)
            }
        }

        scenario("DISCONNECT from active node sends it to passive") {
            val c = new Context
            c.ov.addToActive(c.other)
            c.in.onNext(Disconnect(randomUUID, c.other, c.me))
            c.ov.activeView shouldBe empty
            c.ov.passiveView should contain only c.other
            c.assertNothingEmitted()
        }

        scenario("DISCONNECT from an unknown node does nothing") {
            val c = new Context
            c.ov.addToActive(c.other)
            c.in.onNext(Disconnect(randomUUID, 10, c.me))
            c.ov.activeView should contain only c.other
            c.ov.passiveView shouldBe empty
            c.assertNothingEmitted()
        }

        scenario("SHUFFLE REPLY includes other's nodes into passive view and" +
                 "excludes itself") {
            val c = new Context
            val otherActive = (10 to 15).toSet + c.me
            val expectAdded = (10 to 15).toSet
            c.ov.addToPassive(Set(3))
            c.in.onNext(ShuffleReply(randomUUID, c.other, c.me, otherActive))
            c.ov.passiveView should contain theSameElementsAs (expectAdded + 3)
            c.ov.activeView shouldBe empty
            c.assertNothingEmitted()
        }

        scenario("SHUFFLE REQUEST results in a forward") {
            val c = new Context
            val actives = (10 to 15).toSet + c.me // me should be ignored
            val passives = (20 to 25).toSet + c.me
            val shuffleReq = ShuffleRequest(randomUUID, c.other, c.me, 4,
                                            actives, passives)

            // Prepare an active view that will trigger a fwd
            c.ov.addToActive(10)
            c.ov.addToActive(c.other)

            for (n <- 1 to 1000) {
                c.in.onNext(shuffleReq)
                c.out.getOnCompletedEvents shouldBe empty
                c.out.getOnErrorEvents shouldBe empty

                c.out.getOnNextEvents should have size n
                val fwd = c.out.getOnNextEvents.head
                                               .asInstanceOf[ShuffleRequest]
                fwd.source shouldBe c.me
                fwd.dest shouldBe 10           // never c.other
                fwd.ttl shouldBe shuffleReq.ttl - 1
                fwd.active should contain theSameElementsAs shuffleReq.active
                fwd.passive should contain theSameElementsAs shuffleReq.passive

                val expectPassive = actives -- c.ov.activeView - c.me
                c.ov.passiveView should contain theSameElementsAs expectPassive
                c.ov.activeView should contain only (10, c.other)
            }
        }

        scenario("SHUFFLE REQUEST results in an accept") {
            val c = new Context
            val shuffled = (100 to 115).toSet + c.me // me should be ignored
            val shuffleReq = ShuffleRequest(randomUUID, c.other, c.me, 0,
                                            shuffled, Set.empty)

            // Fill the active view with elements to exchange
            10 until (10 + c.ov.maxActiveSize) foreach c.ov.addToActive
            c.ov.isActiveFull shouldBe true
            c.ov.passiveView shouldBe empty

            c.in.onNext(shuffleReq)

            c.out.getOnCompletedEvents shouldBe empty
            c.out.getOnErrorEvents shouldBe empty

            c.out.getOnNextEvents should have size 1
            c.ov.passiveView should contain theSameElementsAs shuffled - c.me
            val msg = c.out.getOnNextEvents.head.asInstanceOf[ShuffleReply]
            msg.source shouldBe c.me
            msg.dest shouldBe c.other

            // all in the active sample should be in the overlay's active view
            msg.active should have size Math.min(c.ov.activeSize, shuffled.size)
            msg.active.filterNot(c.ov.activeView.contains) shouldBe empty
        }

        scenario("JOIN ignores messages from the same node") {
            val c = new Context
            c.in.onNext(Join(randomUUID, c.me, c.me + 1))
            c.ov.activeSize shouldBe 0
            c.ov.passiveSize shouldBe 0
            c.assertNothingEmitted()
        }

        scenario("JOIN sends disconnect to dropped nodes") {
            val c = new Context
            val initialActive = 10 until (10 + c.ov.maxActiveSize)

            initialActive foreach c.ov.addToActive
            c.ov.isActiveFull shouldBe true
            c.ov.passiveView shouldBe empty

            val join = Join(randomUUID, c.other, c.me)
            c.in.onNext(join)
            c.ov.isActiveFull shouldBe true

            // The sender was added to the active view
            c.ov.activeView should contain (c.other)
            c.ov.passiveView should have size 1
            initialActive should contain (c.ov.passiveView.head)

            // We expect one fwdJoin message less than the active view, because
            // the source of the join gets added to the active set and won't
            // get the fwdJoin.  We add an extra message for the disconnect
            // sent to the element that gets dropped from the active view,
            // as it's full when the join arrives.
            c.out.getOnNextEvents should have size initialActive.size
            val disconn = c.out.getOnNextEvents.last.asInstanceOf[Disconnect]
            disconn.source shouldBe c.me
            initialActive should contain (disconn.dest)
            c.ov.activeView shouldNot contain (disconn.dest)

            val fwdJoins = c.out.getOnNextEvents.take(initialActive.size - 1)
            val receivers = fwdJoins.map ( _.asInstanceOf[ForwardJoin] )
                                    .map { fwdJoin =>
                                         fwdJoin.source shouldBe c.me
                                         fwdJoin.ttl shouldBe FORWARD_JOIN_TTL
                                         fwdJoin.joinedNode shouldBe join.source
                                         fwdJoin.dest
                                    }

            // Any receiver must have been in the initial active view
            receivers -- initialActive shouldBe empty
        }
    }
}
