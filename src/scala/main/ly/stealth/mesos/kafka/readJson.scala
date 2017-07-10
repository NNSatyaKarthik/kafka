package ly.stealth.mesos.kafka

import org.apache.mesos.Protos.TaskInfoOrBuilder
import org.apache.mesos.Protos.Value.Scalar

import scala.io.Source
import scala.util.parsing.json.{JSON, JSONObject}
import org.apache.mesos.Protos._
/**
  * Created by nnarikimilli on 6/23/17.
  */
object readJson {

  var executorMap = Map[String, Any]()
  def getResourceInfo(resources: Any) = {
    def getResource(resource: Map[String, Any]): Resource = {
      val resourceBuilder = Resource.newBuilder()
        .setName(resource("name").asInstanceOf[String])
        .setType(resource("type").asInstanceOf[String] match {
          case "SCALAR" => Value.Type.SCALAR
          case "SET" => Value.Type.SET
          case "RANGES" => Value.Type.RANGES
        })
      val res = resource("type").asInstanceOf[String] match {
        case "SCALAR" => resourceBuilder.setScalar(Scalar.newBuilder().setValue(resource("scalar").asInstanceOf[Map[String, Any]]("value").asInstanceOf[Double]))
          //TODO support SET AND RANGES types
//        case "SET" => resourceBuilder.setSet(Value.Set.newBuilder().addAllItem(resource("set").asInstanceOf[List[String]].).build())
        case "RANGES" => resourceBuilder.setRanges(Value.Ranges.newBuilder.addRange(Value.Range.newBuilder().setBegin(resource("ranges").asInstanceOf[Long]).setEnd(resource("value").asInstanceOf[Long])))
        case _ =>
          println("Supporting only SCALAR as of now..")
          None
      }
      if(res == None) {
        return null
      }
      resourceBuilder.build()
    }

    var resourceInfo: List[Resource] = List[Resource]()
    resources match {
      case None =>
        resourceInfo ::= Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR).setScalar(Scalar.newBuilder().setValue(0.25)).build()
        resourceInfo ::= Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR).setScalar(Scalar.newBuilder().setValue(128)).build()
      case ress:List[Any] =>
        for(resource <- ress){
          resourceInfo ::= getResource(resource.asInstanceOf[Map[String,Any]])
        }
    }
    resourceInfo
  }

  def getCommandInfo(commandInfo: Any): CommandInfo.Builder = {
    commandInfo match {
      case command:Map[String, Any] =>
        val cmdInfo = CommandInfo.newBuilder()
        cmdInfo.setValue(command("value").asInstanceOf[String])
        cmdInfo.setShell(command.getOrElse("shell", true).asInstanceOf[String] == "true")
        if(command.contains("uris")){
          for(uri <- command("uris").asInstanceOf[List[Any]]) {
            val urib = uri match {
              case uri_map: Map[String, Any] =>
                val urib = CommandInfo.URI.newBuilder()
                urib.setValue(uri_map("value").asInstanceOf[String])
                if(uri_map.contains("cache")) urib.setCache(uri_map("cache").asInstanceOf[Boolean])
                urib.setExecutable(uri_map.getOrElse("executable", false).asInstanceOf[Boolean])
                urib.setExtract(uri_map.getOrElse("extract", true).asInstanceOf[Boolean])
                urib
            }
            cmdInfo.addUris(urib.build())
//            println(urib.build())
          }
        }
        cmdInfo
    }
  }

  def getLabels(labels: Any): Labels.Builder ={
    val labelsBuilder = Labels.newBuilder()
    labels match {
      case lbls:List[Any] =>
        for(label <- lbls){
          label match {
            case lbl:Map[String, String] =>
              labelsBuilder.addLabels(Label.newBuilder().setKey(lbl("key")).setValue(lbl("value")).build())
          }
        }
        labelsBuilder
    }

  }
  def loadExecutors(fileName:String) = {
    val executors:List[Map[String, Any]] = JSON.parseFull(Source.fromFile(fileName).mkString) match {
      case Some(l) =>
        l.asInstanceOf[List[Map[String, Any]]]
      case None => List()
    }
    for(executor <- executors){
      var dataMap = Map[String, Any]()
      val exec:Map[String, Any] = executor("executor").asInstanceOf[Map[String, Any]]
      dataMap += ("name" -> exec("name").asInstanceOf[String])
      dataMap += ("command" -> getCommandInfo(exec("command")))
      if(exec.contains("resources")) dataMap += ("resources" -> getResourceInfo(exec("resources")))
      if(exec.contains("labels")) dataMap += ("labels" -> getLabels(exec("labels")))
      executorMap += (dataMap("name").asInstanceOf[String] -> dataMap)
    }
  }

  def printExecutors(): Unit = {
    for(executor <- executorMap.keys){
      val map = executorMap(executor).asInstanceOf[Map[String,Any]]
      println("name: ", map("name"))
      println("command: ", map("command"))
      println("resources: ", map.getOrElse("resources", "NOTFOUND"))
      println("labels: ", map.getOrElse("labels", "NOTFOUND"))
      println("-------------------------------------------------------")
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 1) println("error .. expected a configuration file..")

    loadExecutors(args(0))
    printExecutors()
  }
}
