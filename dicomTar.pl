#!/usr/bin/perl
# J-Sebastian Muehlboeck 2006
# sebas@bic.mni.mcgill.ca
# Archive your dicom data using DICOM::DCMSUM 
# Tar and gzip dicom files and retar them with pertaining summary and creation log
# @VERSION : $Id: dicomTar.pl 9 2007-12-18 22:26:00Z jharlap $

use strict;
use FindBin;
use Getopt::Tabular;
use FileHandle;
use File::Basename;
use File::Find;
use Cwd qw/ abs_path /;
use Socket;
use Sys::Hostname;
use lib "$FindBin::Bin";
use Math::Round;

use DICOM::DCMSUM;
use DB::DBI;

# version info from cvs
my $version = 0;
my $versionInfo = sprintf "%d", q$Revision: 9 $ =~ /: (\d+)/;
# If thing will be done differently this has to change!
my $tarTypeVersion = 1;
# Set stuff for GETOPT
my ($dcm_source, $targetlocation);
my $verbose    = 0;
my $profile    = undef;
my $neurodbCenterName = undef;
my $clobber    = 0;
my $noappend     = 0;
my $dbase      = 0;
my $todayDate  = 0;
my $mri_upload_update =0;
my $Usage = "------------------------------------------


  Author    :        J-Sebastian Muehlboeck
  Date      :        2006/10/01
  Version   :        $versionInfo


WHAT THIS IS:

A tool for archiving DICOM data. Point it to a source dir and provide a target dir which will be the archive location. 
- If the source contains only one valid STUDY worth of DICOM it will create a descriptive summary, a (gzipped) DICOM tarball 
  The tarball with the metadata and a logfile will then be retarred into the final TARCHIVE. 
- md5sums are reported for every step
- It can also be used with a MySQL database.

Usage:\n\t $0 </PATH/TO/SOURCE/DICOM> </PATH/TO/TARGET/DIR> [options]
\n\n See $0 -help for more info\n\n";

my @arg_table =
    (
     ["Input and database options", "section"],
     ["-today", "boolean", 1,     \$todayDate, "Use today's date for archive name instead of using acquisition date."],
     ["-database", "boolean", 1,  \$dbase, "Use a database if you have one set up for you. Just trying will fail miserably"],
     ["-mri_upload_update", "boolean", 1,  \$mri_upload_update, "update the mri_upload table by inserting the correct tarchiveID"],
     ["-clobber", "boolean", 1,   \$clobber, "Use this option only if you want to replace the resulting tarball!"],
     ["-noappend", "boolean", 0,   \$noappend, "Use this option only if you want to not append!"],
     ["-profile","string",1, \$profile, "Specify the name of the config file which resides in .loris_mri in the current directory."],
     ["-centerName","string",1, \$neurodbCenterName, "Specify the symbolic center name to be stored alongside the DICOM institution."],
     ["General options", "section"],
     ["-verbose", "boolean", 1,   \$verbose, "Be verbose."],
     ["-version", "boolean", 1,   \$version, "Print cvs version number and exit."],
     );

GetOptions(\@arg_table, \@ARGV) ||  exit 1;

if ($version) { print "Version: $versionInfo\n"; exit; }

# checking for profile settings
if(-f "$ENV{LORIS_CONFIG}/.loris_mri/$profile") {
	{ package Settings; do "$ENV{LORIS_CONFIG}/.loris_mri/$profile" }
}
if ($profile && !@Settings::db) {
    print "\n\tERROR: You don't have a configuration file named '$profile' in:  $ENV{LORIS_CONFIG}/.loris_mri/ \n\n"; exit 33;
} 
# The source and the target dir have to be present and must be directories. The absolute path will be supplied if necessary
if(scalar(@ARGV) != 2) { print "\nError: Missing source and/or target\n\n".$Usage; exit 1; } $dcm_source = abs_path($ARGV[0]); $targetlocation = abs_path($ARGV[1]);
#if (!$dcm_source || !$targetlocation) { print $Usage; exit 1; }
if (-d $dcm_source && -d $targetlocation) { $dcm_source =~ s/^(.*)\/$/$1/; $targetlocation =~ s/^(.*)\/$/$1/; } 
else { print "\nError: source and target must be existing directories!!\n\n"; exit 1; }

# The tar target 
my $totar = basename($dcm_source);
print "Source: ". $dcm_source . "\nTarget: ".  $targetlocation . "\n\n" if $verbose;
my $ARCHIVEmd5sum = 'Provided in database only';


# establish database connection if database option is set
my $dbh; if ($dbase) { $dbh = &DB::DBI::connect_to_db(@Settings::db); print "Testing for database connectivity.\n" if $verbose; $dbh->disconnect();  print "Database is available.\n\n" if $verbose;}

# ***************************************    main    *************************************** 
#### get some info about who created the archive and where and when
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
my $date            = sprintf("%4d-%02d-%02d %02d:%02d:%02d\n",$year+1900,$mon+1,$mday,$hour,$min,$sec);
my $today           = sprintf("%4d-%02d-%02d",$year+1900,$mon+1,$mday);
my $hostname        = inet_ntoa(scalar(gethostbyname(hostname() || 'localhost'))); #`hostname -f`; # fixme specify -f for fully qualified if you need it.
my $system          = `uname`;


# Remove .DS_Store from dcm_source directory if exist
if (-e $dcm_source . "/.DS_Store") {
    my $cmd = "rm $dcm_source/.DS_Store";
    system($cmd);
}

# create new summary object
my $summary = DICOM::DCMSUM->new($dcm_source,$targetlocation);
# determine the name for the summary file
my ($sumTypeVersion,$studyUnique,$creator,$sumTypeVersion,$DICOMmd5sum,$zipsum,$metafile,$finalTarget,$metafile,$metaname);
my ($seriesNum, $sequName,  $echoT, $repT, $invT, $seriesName, $sl_thickness, $phaseEncode, $seriesUID, $modality, $num);
my $echoTRound;
my $ind = 0;
my $update          = 1 if $clobber;

=pod
if ($dbase) {
        $dbh = &DB::DBI::connect_to_db(@Settings::db);
         # EXIT if a seriesUID is already in the database
        print "\nChecking for duplicate sequences between the upload and those inserted into the database\n" if $verbose;
        my $duplicate_sequence_count;
        my $sequence_count;
        my @acqu_List = @{$summary->{acqu_List}};
        my $acqu_List_count = $#acqu_List+1;

        foreach my $acq (@acqu_List) {
            # insert the series
            my ($seriesNum, $sequName,  $echoT, $repT, $invT, $seriesName, $sl_thickness, $phaseEncode, $seriesUID, $modality, $num) = split(':::', $acq);

            (my $query_seriesuid = <<QUERY) =~ s/\n/ /gm;
              SELECT
                TarchiveSeriesID,
                TarchiveID,
                SeriesNumber,
                SeriesDescription
              FROM
                tarchive_series
              WHERE
                SeriesUID=?
QUERY
            my $sth_seriesuid = $dbh->prepare($query_seriesuid);
            $sth_seriesuid->execute($seriesUID);
            my @row = $sth_seriesuid->fetchrow_array();


            if($sth_seriesuid->rows > 0) {
                $duplicate_sequence_count++;
                # if there is an entry, and no clobber, send a message to the user that this sequence has been already inserted so it won't be updated
                print "\nWARNING:\nThe sequence \'$row[3]\' has already been inserted to the database. \n".
                      "The Series Number is \'$row[2]\', the TarchiveSeriesID is \'$row[0]\' and the TarchiveID is \'$row[1]\'.\n" .
                      "Skipping insertion of this sequence. \n";
            }
            if ($duplicate_sequence_count >= 1) {
                print "\nERROR1:\nSome sequences uploaded have already been inserted into the database. Please re-upload only unique sequences. Exiting. \n";
                exit 33;
            }
            else {
                print "\nNo duplicate series found in the upload. Continuing the insertion. \n";
            }
        }
        # End check for an existing seriesUID inserted into the database
}
=cut

$dbh = &DB::DBI::connect_to_db(@Settings::db);
my @seriesUID_inserted;
my @echoT_inserted;
foreach my $acq (@{$summary->{acqu_List}}) {
    ($seriesNum, $sequName,  $echoT, $repT, $invT, $seriesName, $sl_thickness, $phaseEncode, $seriesUID, $modality, $num) = split(':::', $acq);

    (my $query_seriesuid = <<QUERY) =~ s/\n/ /gm;
      SELECT
        ts.TarchiveSeriesID,
        ts.TarchiveID,
        ts.SeriesNumber,
        ts.SeriesDescription,
        t.SourceLocation
      FROM
        tarchive_series ts
      JOIN
        tarchive t
      ON
        t.TarchiveID = ts.TarchiveID
      WHERE
        ts.SeriesUID=?
      AND
        ts.EchoTime=?
      AND
        t.SourceLocation<>?
QUERY
    my $sth_seriesuid = $dbh->prepare($query_seriesuid);
    $sth_seriesuid->execute($seriesUID, $echoT, $dcm_source);
    my @row = $sth_seriesuid->fetchrow_array();


    if($sth_seriesuid->rows == 0) { # not in database from a previous upload
        if ((in_array($seriesUID, \@seriesUID_inserted)==0) || (in_array($echoT, \@echoT_inserted)==0)) { #only insert unique echoT and seriesUID from this upload
            $echoTRound = round($echoT);
            if ($ind == 0) {
                $update = $clobber
            } else {
                $update = 0;
            }

            my $acqcount = 1;
        #    my $acqcount = acquisition_count($summary->{acqu_List}, $seriesUID);
            my $dcmcount = dcm_count($summary->{dcminfo}, $seriesUID);
            my $filecount = file_count($summary->{dcminfo}, $seriesUID);

            $summary->{totalcount}        = $filecount;
            $summary->{dcmcount}          = $dcmcount;
            $summary->{acquisition_count} = $acqcount;


            $metaname = $summary->{'metaname'};
            # get the summary type version
            $sumTypeVersion = $summary->{'sumTypeVersion'};
            # get the unique study ID
            $studyUnique = $summary->{'studyuid'};
            $creator         = $summary->{user};
            $sumTypeVersion  = $summary->{sumTypeVersion};

            my $byDate;
            my $seriesNum2 = $seriesNum . "_" . $echoTRound;
            # Determine how to name the archive... by acquisition date or by today's date.
            if ($todayDate) { $byDate = $today; } else { $byDate = $summary->{header}->{scandate}; } # wrap up the archive
            $finalTarget = "$targetlocation/DCM_${byDate}_$summary->{metaname}_$seriesNum2.tar";
            print "Target is: " . $finalTarget . "\n";
            if (-e $finalTarget && !$clobber) { print "\nTarget exists. Use clobber to overwrite!\n\n"; exit 2; }


            # read acquisition metadata into variable
            my $metaname_seriesnum = $metaname . "_" . $seriesNum . "_" . $echoTRound;
            $metafile = "$targetlocation/$metaname_seriesnum.meta";
            open META, ">$metafile";
            META->autoflush(1);
            select(META);
            $summary->dcmsummary($seriesNum);
            my $metacontent = $summary->read_file("$metafile");

            # write to STDOUT again
            select(STDOUT);

            # get rid of newline
            chomp($hostname,$system);

            #### create tar from right above the source
            chdir(dirname($dcm_source));
            print "You will archive the dir\t\t: $totar\n" if $verbose;

            ### get only the files with a specific series_description:

            my $totar_seriesnum = $totar."_".$seriesNum."_".$echoTRound;

            my @file_list;
            find(
                sub {
                    return unless -f;    #Must be a file
                    push @file_list, $File::Find::name;
                },
                $dcm_source
            );

            my ($dicom_file, $cmd, $series_instanceUID, $echo_time, $command, $l, $t, $metalist_seriesnum);
            $command = "cd $dcm_source; tar cf $targetlocation/$totar_seriesnum.tar ";
            $metalist_seriesnum = $metaname . "_" . $seriesNum . "_" . $echoTRound;
            $metalist_seriesnum = "$targetlocation/$metalist_seriesnum.txt";
            open METALIST, ">$metalist_seriesnum";
            METALIST->autoflush(1);
            select(METALIST);
            foreach (@file_list) {
                $dicom_file = $_;
                $cmd          = "dcmdump $dicom_file | grep SeriesInstanceUID";
                $series_instanceUID =  `$cmd`;
                ($l,$series_instanceUID ,$t) = split /\[(.*?)\]/, $series_instanceUID ;
                #print $series_instanceUID . "\lsn";
                $cmd          = "dcmdump $dicom_file | grep EchoTime";
                $echo_time =  `$cmd`;
                ($l,$echo_time ,$t) = split /\[(.*?)\]/, $echo_time ;
                #print $echo_time . "\n";
                $dicom_file =~ s/$dcm_source//g;
                $dicom_file =~ s/^\///;
                #$dicom_file =~ s/$totar//g;
                if ($series_instanceUID  eq $seriesUID && $echo_time eq $echoT) {
                # write list of files to be Tarred in a separate .txt to avoid character limit on the Tar command
                print METALIST "$dicom_file\n";
                select(STDOUT);
                }
            }
            $command = $command . " -T " . $metalist_seriesnum;
            close METALIST;

            # tar contents into tarball
            select(META);
            select(STDOUT);
            print "\nYou are creating a tar with the following command: \n$command\n" if $verbose;
            `$command`;

            # chdir to targetlocation create md5sums gzip and wrap the whole thing up again into a retarred archive
            chdir($targetlocation);
            print "\ngetting md5sums and gzipping!!\n" if $verbose;
            print $totar_seriesnum . "\n";
            $DICOMmd5sum = DICOM::DCMSUM::md5sum($totar_seriesnum.".tar"); #`md5sum $totar.tar`;
            `gzip -nf $totar_seriesnum.tar`;
            $zipsum =  DICOM::DCMSUM::md5sum($totar_seriesnum.".tar.gz");

            # create tar info for the tarball NOT  containing md5 for archive tarball
            open TARINFO, ">$totar_seriesnum.log";
            select(TARINFO);
            &archive_head;
            close TARINFO;
            select(STDOUT);
            my $tarinfo = &read_file("$totar.log");

            my $retar = "tar cvf DCM\_$byDate\_$totar_seriesnum.tar $totar_seriesnum.meta $totar_seriesnum.log $totar_seriesnum.tar.gz";
            `$retar`;
            print "Just after the retar\n";
            $ARCHIVEmd5sum =  DICOM::DCMSUM::md5sum("DCM\_$byDate\_$totar_seriesnum.tar");

            # create tar info for database containing md5 for archive tarball
            open TARINFO, ">$totar_seriesnum.log";
            select(TARINFO);
            &archive_head;
            close TARINFO;
            select(STDOUT);
            $tarinfo = &read_file("$totar_seriesnum.log");
            print  $tarinfo if $verbose;


            # if -dbase has been given create an entry based on unique studyID
            # Create database entry checking for already existing entries...
            my $success;
            if ($dbase) {
                $dbh = &DB::DBI::connect_to_db(@Settings::db);
                print "\nAdding archive info into database\n" if $verbose;
                my $ArchiveLocation = $finalTarget;
                $ArchiveLocation    =~ s/$targetlocation\/?//g;
                $success            = $summary->database($dbh, $metaname, $update, $noappend, $tarTypeVersion, $tarinfo, $DICOMmd5sum, $ARCHIVEmd5sum, $ArchiveLocation, $neurodbCenterName, $ind);
            }


            # now report database failure (was not above to ensure temp files were erased)
            if ($dbase) {
                if ($success) { print "\nDone adding archive info into database\n" if $verbose; }
                else { print "\nThe database command failed\n"; exit 22; }
            }

            # call the updateMRI_upload script###
            if ($mri_upload_update) {
                my $script =  "updateMRI_Upload.pl"
                         . " -profile $profile -globLocation -tarchivePath $finalTarget"
                         . " -sourceLocation $dcm_source";
                my $output = system($script);
                if ($output!=0)  {
                    print "\n\tERROR: the script updateMRI_Upload.pl has failed \n\n";
                    exit 33;
                }
            # delete tmp files
            print "\nRemoving temporary files from target location\n\n" if $verbose;
            `rm -f $totar_seriesnum.tar.gz $totar_seriesnum.meta $totar_seriesnum.log`;
            }
            push(@seriesUID_inserted, $seriesUID);
            push(@echoT_inserted, $echoT);
        }
        $ind++;
     }
     else { #series in database
        print "\nERROR2:\nSome sequences uploaded have already been inserted into the database. Please re-upload only unique sequences. Exiting. \n";
        exit 33;
     }
}# end of for each acquisition

# delete tmp files
print "\nRemoving temporary files from target location\n\n" if $verbose;
`rm -f $totar.tar.gz $totar.meta $totar.log`;

exit 0;
# **************************************************************************************************************************  
=pod 
################################################
print tarchive header
################################################
=cut 
sub archive_head {
    $~ = 'FORMAT_HEADER';
    write();
}

format FORMAT_HEADER =

* Taken from dir                   :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $dcm_source,
* Archive target location          :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $finalTarget,
* Name of creating host            :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $hostname,                                
* Name of host OS                  :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $system,
* Created by user                  :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $creator,                                
* Archived on                      :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $date,
* dicomSummary version             :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $sumTypeVersion,
* dicomTar version                 :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $tarTypeVersion,
* md5sum for DICOM tarball         :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $DICOMmd5sum,
* md5sum for DICOM tarball gzipped :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $zipsum,
* md5sum for complete archive      :    @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                                      $ARCHIVEmd5sum,
.


=pod 
################################################
Read file content into variable
################################################
=cut    
sub read_file {
    my $file = shift;
    my $content;
    open CONTENT, "$file";
    while ( <CONTENT> ) {
	$content = $content . $_;
    }
    close CONTENT;
    return $content;
}

=pod
# Figure out the total number of acquisitions
sub acquisition_count {
    my ($acq) = shift;
    my ($seriesUID) = shift;
    my @ac = @{$acq};
    my $count = @ac;
    return $count;
}
=cut

# Figure out the total number of files
sub file_count {

    my ($acq) = shift;
    my ($seriesUID) = shift;
    my $count = 0;
    my @ac = @{$acq};
    foreach my $file (@ac) {
	    if($file->[24] eq $seriesUID) {
	            $count++;
	    }
	}
    return $count;
}

sub dcm_count {

    my ($acq) = shift;
    my ($seriesUID) = shift;
    my @ac = @{$acq};

    my $count = 0;
    foreach my $file (@ac) {
	    if($file->[24] eq $seriesUID) {
	        if($file->[21]) { # file is dicom
	            $count++;
	        }
	    }
    }
    if ($count == 0) {
	print "\n\t The target directory does not contain a single DICOM file. \n\n\n";
	    exit 33;
    }
    else { return $count;}
}

sub in_array {
  my ($item, $array) = @_;
  my %hash = map { $_ => 1 } @$array;
  if ($hash{$item}) { return 1; } else { return 0; }
}
