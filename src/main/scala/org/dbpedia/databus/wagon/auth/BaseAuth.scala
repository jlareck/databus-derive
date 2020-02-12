package org.dbpedia.databus.wagon.auth

class BaseAuth(user: String, psw: String) extends WagonAuth {

  val getUser: String = user
  val getPsw: String = psw
}
