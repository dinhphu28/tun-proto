package io.tunproto.scala

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TunnelServerAuthSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private def server(opts: TunnelOptions) = TunnelServer(opts)(system).route

  "TunnelServer auth" should {

    "reject a WS upgrade with no Authorization header (401)" in {
      val route = server(TunnelOptions(apiKeys = Set("secret")))
      val wsClient = WSProbe()
      WS("/tunnels", wsClient.flow) ~> route ~> check {
        status shouldBe StatusCodes.Unauthorized
      }
    }

    "reject a bad Bearer token (401)" in {
      val route = server(TunnelOptions(apiKeys = Set("secret")))
      val wsClient = WSProbe()
      WS("/tunnels", wsClient.flow).addHeader(Authorization(OAuth2BearerToken("wrong"))) ~> route ~> check {
        status shouldBe StatusCodes.Unauthorized
      }
    }

    "accept a good Bearer token and upgrade" in {
      val route = server(TunnelOptions(apiKeys = Set("secret")))
      val wsClient = WSProbe()
      WS("/tunnels", wsClient.flow).addHeader(Authorization(OAuth2BearerToken("secret"))) ~> route ~> check {
        isWebSocketUpgrade shouldBe true
      }
    }

    "accept any connection when authDisabled" in {
      val route = server(TunnelOptions(authDisabled = true))
      val wsClient = WSProbe()
      WS("/tunnels", wsClient.flow) ~> route ~> check {
        isWebSocketUpgrade shouldBe true
      }
    }

    "use a custom authenticator" in {
      val route = server(
        TunnelOptions(authenticator = Some((k: String) => scala.concurrent.Future.successful(k == "ok")))
      )
      val good = WSProbe()
      WS("/tunnels", good.flow).addHeader(Authorization(OAuth2BearerToken("ok"))) ~> route ~> check {
        isWebSocketUpgrade shouldBe true
      }
      val bad = WSProbe()
      WS("/tunnels", bad.flow).addHeader(Authorization(OAuth2BearerToken("no"))) ~> route ~> check {
        status shouldBe StatusCodes.Unauthorized
      }
    }

    "reject a request to the wrong path" in {
      val route = server(TunnelOptions(apiKeys = Set("secret")))
      val wsClient = WSProbe()
      WS("/nope", wsClient.flow).addHeader(Authorization(OAuth2BearerToken("secret"))) ~> route ~> check {
        handled shouldBe false
      }
    }
  }

  "TunnelOptions validation" should {
    "require exactly one auth mode" in {
      an[IllegalArgumentException] should be thrownBy TunnelOptions()
      an[IllegalArgumentException] should be thrownBy TunnelOptions(apiKeys = Set("a"), authDisabled = true)
    }
    "reject a too-large maxStreamWindow" in {
      an[IllegalArgumentException] should be thrownBy
        TunnelOptions(apiKeys = Set("a"), maxStreamWindow = 16 * 1024 * 1024)
    }
  }
}
