StartDateTime=20171101000000
EndDateTime=20171111000000

CurrentDateTime=$StartDateTime

URLPrefix="http://data.gdeltproject.org/gdeltv2/"
URLPostfix=".export.CSV.zip"

while [ "$CurrentDateTime" != "$EndDateTime" ];do
	DownloadURL="$URLPrefix$CurrentDateTime$URLPostfix"
	echo "Getting $DownloadURL..."
	$(wget $DownloadURL)
	CurrentDateTime=$(date -j -f %Y%m%d%H%M%S -v+15M $CurrentDateTime +%Y%m%d%H%M%S)
done

$(unzip \*.zip)
$(rm *.zip)
