import AssemblyKeys._ // put this at the top of the file

assemblySettings

mergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*)  => MergeStrategy.first
  case PathList("META-INF", xs @ _*)                 => MergeStrategy.discard
  case PathList(ps @ _*)                             => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}
