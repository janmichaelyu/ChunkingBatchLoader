
let $xqy := 
<xml><![CDATA[
declare variable $allItems as xs:string external;
let $json := xdmp:unquote($allItems)

let $colls := ("Test", "hubinsert")
let $options :=
    <options xmlns="xdmp:document-insert">  
      <permissions>{xdmp:default-permissions()}</permissions>
      <collections>{
        for $coll in $colls
        return <collection>{$coll}</collection>
      }</collections>
      <quality>10</quality>
    </options>

for $data in $json/data
let $uri := $data/URI
let $envelope := $data/item
let $insert := xdmp:document-insert($uri, $envelope, $options)
return ()
]]></xml>/text()

let $check := let $db-name := xdmp:database-name(xdmp:database()) return if ($db-name eq "Modules") then () else error((), "modules only")
let $insert := xdmp:document-insert("/batchHubInput.xqy", text{$xqy})
return "done!"
;
doc("/batchHubInput.xqy")
