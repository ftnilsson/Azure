#Server side storage copy

function RemoveAllBlobsInContainer([string] $container) {
    ForEach ($azcontainer in Get-AzureStorageContainer $container){
        ForEach ($azureblob in  Get-AzureStorageBlob -Container $azcontainer.Name){
            ForEach-Object {Remove-AzureStorageBlob -Blob $azureblob.Name -Container $azcontainer.Name }
        }
    }
}

function RemoveAllContainers{
param ($storagecontext, $containers)
    ForEach ($azcontainer in $containers){
     Remove-AzureStorageContainer $azcontainer.Name -Context $storagecontext -Force
        }
}

function Insert-Records
{
    param ($table, $entities)

    $batches = @{}

    foreach ($entity in $entities)
    {
       if ($batches.ContainsKey($entity.PartitionKey) -eq $false)
       {
           $batches.Add($entity.PartitionKey, (New-Object Microsoft.WindowsAzure.Storage.Table.TableBatchOperation))
       }

       $batch = $batches[$entity.PartitionKey]
       $batch.Add([Microsoft.WindowsAzure.Storage.Table.TableOperation]::InsertOrReplace($entity));

       if ($batch.Count -eq 100)
       {
           $table.CloudTable.ExecuteBatch($batch);
           $batches[$entity.PartitionKey] = (New-Object Microsoft.WindowsAzure.Storage.Table.TableBatchOperation)
       }
    }

    foreach ($batch in $batches.Values)
    {
        if ($batch.Count -gt 0)
        {
            $table.ExecuteBatch($batch);
        }
    }
}

function Copy-Records
{
    param($sourceTable, $targetTable)

    $tableQuery = New-Object 'Microsoft.WindowsAzure.Storage.Table.TableQuery'
    
    [Microsoft.WindowsAzure.Storage.Table.TableContinuationToken]$token = $null
        
    do
    {
        $segment = $sourceTable.CloudTable.ExecuteQuerySegmented($tableQuery, $token);
        $token = $segment.ContinuationToken

        Insert-Records $targetTable $segment.Results

        $count = $segment.Results.Count
        Write-Host "Copied $count records"
    } while ($token -ne $null)
   
}

function Get-Table
{
    param($storageContext, $tableName, $createIfNotExists)

    $table = Get-AzureStorageTable $tableName -Context $storageContext -ErrorAction Ignore
    if ($table -eq $null)
    {
        if($createIfNotExists -eq $false)
	{
	    return $null
	}
        
        $table = New-AzureStorageTable $tablename -Context $storageContext
    }
    
    return $table.CloudTable
}

function New-Environment()
{
  param ($uri, $key)

  $environment = new-object PSObject

  $environment | add-member -type NoteProperty -Uri First -Value $uri
  $environment | add-member -type NoteProperty -Key Last -Value $key
  return $environment 
}


function GetEnvironmentConfiguration
{
     param([string]$environment)
     
     [hashtable]$Return = @{} 
 
     if ($environment.ToUpperInvariant() -eq "DEV") {
         $Return.Key="ACCOUNT KEY GOES HERE"
         $Return.Context=New-AzureStorageContext –StorageAccountName "yourdev" -StorageAccountKey $Return.Key        
     }   
     elseif ($environment.ToUpperInvariant() -eq "STAGING") {
         $Return.Key="ACCOUNT KEY GOES HERE"
         $Return.Context=New-AzureStorageContext –StorageAccountName "yourstaging" -StorageAccountKey $Return.Key      
     }     
     elseif ($environment.ToUpperInvariant() -eq "PROD") {
         $Return.Key="ACCOUNT KEY GOES HERE"
         $Return.Context= New-AzureStorageContext –StorageAccountName "yourprod" -StorageAccountKey $Return.Key        
     }     
     elseif ($environment.ToUpperInvariant() -eq "LOCAL") {
     #https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator default key for azure local storage
          $Return.Key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        $Return.Context=New-AzureStorageContext -Local        
     }
     return $Return
}

$requestedEnvironment = Read-Host -Prompt 'Input the source you want to copy data from. Valid values DEV,STAGING,PROD'
$requestedDestinationEnvironment = Read-Host -Prompt 'Input the destination you want to copy data To. Valid values LOCAL,DEV,STAGING,PROD'

$SourceEnvironment =GetEnvironmentConfiguration $requestedEnvironment
$DestinationEnvironment = GetEnvironmentConfiguration $requestedDestinationEnvironment
 
New-Item -ItemType Directory -Force -Path c:\temp\azcopy

$maxReturn=100
$total = 0
$token = $null
$destinationtoken = $null
$slash="/"

$excludedContainers="" #name of any blob containers to exluded
$excludedTables=""  #name of any tables to exluded

$block = {
        Param($sourceuri,$source,$sourcekey,$type,$destination,$destinationkey)
        $azPath = “C:\Program Files (x86)\Microsoft SDKs\Azure\AzCopy”
        Set-Location $azPath

        if (!$destination.EndsWith("/")){
            $destinationuri=$destination+"/"+$source 
        }
        else{
            $destinationuri=$destination+$source 
        }

        if ($type -eq 'Blob') {   
            if ($sourceuri -contains $source) {
                $Result=.\AzCopy.exe /Source:$sourceuri /Dest:$destinationuri /SourceKey:$sourcekey /SourceType:$type /DestKey:$destinationkey /DestType:$type /S /Y /Z:c:\temp\azcopy\$source
      
            }
            else{    
                $Result=.\AzCopy.exe /Source:$sourceuri$source  /Dest:$destinationuri /SourceKey:$sourcekey /SourceType:$type /DestKey:$destinationkey /DestType:$type /S /Y /Z:c:\temp\azcopy\$source
            }
        }
        else
        {
            Write-Output "Trying to copy table:"$sourceuri$source  
            $Result=.\AzCopy.exe /Source:$sourceuri$source /Dest:"c:\temp\tables\"$source /SourceKey:$sourcekey /SourceType:$type /Y /Z:c:\temp\tables\$source
            Write-Output "result:"$Result
            Write-Output "result[0]:"$Result[0]
            $destinationtable="$destination/$source"
            Write-Output "destinationtable:" $destinationtable

            $mainfestfile=$Result[0].Substring($Result[0].IndexOf("""")+1,$Result[0].LastIndexOf("""")-$Result[0].IndexOf("""")-1)
            $Result=.\AzCopy.exe /Source:"c:\temp\tables\"$source  /Dest:$destinationtable /DestKey:$destinationkey /DestType:$type /Y /Z:c:\temp\tables\$source /Manifest:$mainfestfile /EntityOperation:InsertOrReplace
        }
    }

#Cleanup journals
Remove-item c:\temp\azcopy\* -recurse -force
Remove-item c:\temp\tables\* -recurse -force
#Remove all jobs
Get-Job | Remove-Job -Force
$MaxThreads = 8
Write-Output "Starting copy at $(Get-Date)"
    do
    {
    	$containers = Get-AzureStorageContainer -MaxCount $maxReturn  -ContinuationToken $token -Context $SourceEnvironment.Context
    	$total += $containers.Count
    	if($containers.Length -le 0) { 
    Write-Output "No source containters found - breaking execution."
    	
        break;}
        
        Write-Output "Removing all containers in destination."
    	$destinationcontainers = Get-AzureStorageContainer -Context $DestinationEnvironment.Context | where {$_.Name -notin $excludedContainers}
        RemoveAllContainers $DestinationEnvironment.Context $destinationcontainers;

        Write-Host "Removing azure tables..."
        Get-AzureStorageTable -Context $DestinationEnvironment.Context  * | where {$_.Name -notin $excludedTables} | Remove-AzureStorageTable -Context $DestinationEnvironment.Context -Force -ErrorAction Ignore
        # Start-Sleep -Milliseconds 30000

      Write-Output "Start copying all containers to destination."
      ForEach ($azcontainer in $containers){
       
        While ($(Get-Job -state running).count -ge $MaxThreads){
            Start-Sleep -Milliseconds 1000
        }
        Start-Job -Scriptblock $Block -ArgumentList $SourceEnvironment.Context.BlobEndPoint,$azcontainer.Name,$SourceEnvironment.Key,"Blob",$DestinationEnvironment.Context.BlobEndPoint,$DestinationEnvironment.Key
      }
        	$token = $containers[$containers.Count-1].ContinuationToken;
    } 
    while($token -ne $null)

    #Wait for all jobs to finish.
While ($(Get-Job -State Running).count -gt 0){
    start-sleep 1
}
#Get information from each job.
foreach($job in Get-Job){
    $info= Receive-Job -Id ($job.Id)
    Write-Output "Info from job:" $job "info:" $info
}
#Remove all jobs created.
Get-Job | Remove-Job
$MaxThreads = 2 #Tables reduced due to less performant for emulator service.
Write-Host "Starting coping tables"
            foreach ($sourcetable in  get-azurestoragetable -context $SourceEnvironment.Context ){
                While ($(Get-Job -state running).count -ge $MaxThreads){
            Start-Sleep -Milliseconds 1000
        }
        Start-Job -Scriptblock $Block -ArgumentList $SourceEnvironment.Context.TableEndpoint,$sourcetable.Name,$SourceEnvironment.Key,"Table",$DestinationEnvironment.Context.TableEndpoint,$DestinationEnvironment.Key
    
        }

            #Wait for all jobs to finish.
While ($(Get-Job -State Running).count -gt 0){
    start-sleep 1
}
#Get information from each job.
foreach($job in Get-Job){
    $info= Receive-Job -Id ($job.Id)
    Write-Output "Info from job:" $job "info:" $info
}
#Remove all jobs created.
Get-Job | Remove-Job


Write-Output "Finishing copy at $(Get-Date)"