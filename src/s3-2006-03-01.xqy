xquery version "1.0-ml";

module namespace s3="http://marklogic.com/s3";
declare option xdmp:mapping "false";
declare variable $s3:endpoint := "s3.amazonaws.com" ;
declare variable $s3:endpoint-map := s3:init-endpoint-map() ;


declare %private function s3:init-endpoint-map() as map:map 
{
   let $map := map:map(),
       $_ := map:put($map,"","s3.amazonaws.com" ),
       $_ := map:put($map,"us-west-1",  "s3-us-west-1.amazonaws.com" ),
       $_ := map:put($map,"us-west-2" , "s3-us-west-2.amazonaws.com"),
       $_ := map:put($map,"eu" ,  "s3-eu-west-1.amazonaws.com" ),
       $_ := map:put($map,"ap-southeast-1" , "s3-ap-southeast-1.amazonaws.com" ),
       $_ := map:put($map,"ap-northeast-1","s3-ap-northeast-1.amazonaws.com"),
       $_ := map:put($map,"sa-east-1","s3-sa-east-1.amazonaws.com" )
       
   return $map 
   
};
       
declare %private function s3:get-endpoint( $region as xs:string? , $bucket as xs:string? ) as xs:string 
{
   if( fn:exists( $region ) ) then 
       map:get( $s3:endpoint-map , $region )
   else
       $s3:endpoint

};

declare %private function s3:get-path( $region as xs:string? , $bucket as xs:string? , $object as xs:string ? ) as xs:string 
{
    "/" || fn:string-join( ( $bucket , $object ) , "/" )

};

declare %private function s3:add-sequence-param($params as map:map, $base-name as xs:string, $values as item()*) as empty-sequence() {
    for $value at $position in $values
    let $key := fn:string-join(($base-name, xs:string($position)),".")
    return if (fn:string-length(xs:string($value))) then map:put($params, $key, xs:string($value)) else ()
};

declare %private  function s3:add-param($params as map:map, $key as xs:string, $value as item()) as empty-sequence() {
    if (fn:string-length(xs:string($value))) then map:put($params, $key, xs:string($value)) else ()
};

declare %private function s3:get-uri( $host as xs:string , $path as xs:string ) as xs:string
{
         "https://" || $host || $path 

};

declare %private  function s3:serialize-params($params as map:map) as xs:string {
    let $pairs :=
            for $key in map:keys($params)
            order by $key collation "http://marklogic.com/collation/codepoint"
        return fn:string-join((xdmp:aws-url-encode($key),xdmp:aws-url-encode(xs:string(map:get($params,$key)))),"=")
    return fn:string-join($pairs, "&amp;")
};


declare function s3:get-request(
    $access-key as xs:string, 
    $secret-key as xs:string,
    $region  as xs:string? , 
    $bucket as xs:string ?, 
    $object   as xs:string ?
    )   {
    
    let $host := s3:get-endpoint( $region , $bucket ),
        $path := s3:get-path( $region , $bucket , $object ),
        $timestamp := fn:current-dateTime(),
        $content-type := "" ,
        $content-md5 := "",
        $tz :=  fn:replace(fn:format-dateTime(  $timestamp , "[Z]" ),":",""),
        $xtimestamp := fn:format-dateTime(  $timestamp , "[FNn,*-3], [D] [MNn,*-3] [Y] [H01]:[m01]:[s01] " ) || $tz,
        $string-to-sign := fn:string-join(("GET",$content-md5,$content-type,$xtimestamp,$path),"&#xA;"),
        $signature := xdmp:hmac-sha1( $secret-key,$string-to-sign, "base64"),
        $uri := s3:get-uri( $host , $path ),
        $options := 
        <options xmlns="xdmp:http">
            <format xmlns="xdmp:document-get">xml</format>
            <headers>
               <Date>{$xtimestamp}</Date>
               <Host>{$host}</Host>
               <Authorization>{"AWS " || $access-key || ":" || $signature }</Authorization>
            </headers>
         </options>,
         $result :=  xdmp:http-get( $uri , $options/. )
    return ($path,$uri,$options,$string-to-sign,$result)
};


declare function s3:delete-request(
    $access-key as xs:string, 
    $secret-key as xs:string, 
    $region as xs:string ? , 
    $bucket   as xs:string ? ,
    $object as xs:string
    ) as node()?   {
    
    let $host := s3:get-endpoint( $region , $bucket ),
        $path := s3:get-path( $region , $bucket , $object ),
        $timestamp := fn:current-dateTime(),
        $content-type := "" ,
        $content-md5 := "",
        $path-to-sign := $path ,
        $tz :=  fn:replace(fn:format-dateTime(  $timestamp , "[Z]" ),":",""),
        $xtimestamp := fn:format-dateTime(  $timestamp , "[FNn,*-3], [D] [MNn,*-3] [Y] [H01]:[m01]:[s01] " ) || $tz,
        $string-to-sign := fn:string-join(("DELETE",$content-md5,$content-type,$xtimestamp,$path-to-sign),"&#xA;"),
        $signature := xdmp:hmac-sha1( $secret-key,$string-to-sign, "base64"),
        $uri := s3:get-uri( $host , $path ),
        $options := 
        <options xmlns="xdmp:http">
                    <format xmlns="xdmp:document-get">xml</format>
            <headers>
               <Date>{$xtimestamp}</Date>
               <Authorization>{"AWS " || $access-key || ":" || $signature }</Authorization>
               <Host>{ $host }</Host>
            </headers>
         </options>,
         $result :=  xdmp:http-delete($uri, $options ) 
    return  <all uri="{$uri}"><signed>{ $string-to-sign }</signed>{$options}<result>{$result[1]}</result><result>{$result[2]}</result></all>
};

declare function  s3:list-buckets( 
    $access-key as xs:string, 
    $secret-key as xs:string  )  
    {
       s3:get-request( $access-key , $secret-key ,() , (), ()  )
};

declare function  s3:bucket-delete( 
    $access-key as xs:string, 
    $secret-key as xs:string, 
    $region as xs:string ?,
    $bucket as xs:string )  as node()?
    {
       s3:delete-request( $access-key , $secret-key ,$region, $bucket , "" )
};


declare function  s3:bucket-delete-cors( 
    $access-key as xs:string, 
    $secret-key as xs:string,
       $region as xs:string ?,
    $bucket as xs:string )  as node()?
    {
       s3:delete-request( $access-key , $secret-key , $region , $bucket ,  "?cors" )
};


declare function  s3:bucket-delete-tagging( 
    $access-key as xs:string, 
    $secret-key as xs:string,
        $region as xs:string ?,
    $bucket as xs:string )  as node()?
    {
       s3:delete-request( $access-key , $secret-key , $region , $bucket  , "?tagging"  )
};


declare function  s3:bucket-delete-website( 
    $access-key as xs:string, 
    $secret-key as xs:string,
    $region as xs:string ?,
    $bucket as xs:string )  as node()?
{
       s3:delete-request( $access-key , $secret-key , $region , $bucket ,"?website" )
};

declare function  s3:bucket-list-objects( 
    $access-key as xs:string, 
    $secret-key as xs:string,
    $region as xs:string ?,
    $bucket as xs:string )  
{
       s3:get-request( $access-key , $secret-key , $region , $bucket  , ()  )
};

