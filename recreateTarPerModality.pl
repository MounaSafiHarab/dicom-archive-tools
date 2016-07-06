#! /usr/bin/perl

use strict;
use warnings;
use Getopt::Tabular;
use File::Temp qw/ tempdir /;
use File::Basename;
use File::Find;
use Cwd;
use NeuroDB::DBI;
use NeuroDB::MRIProcessingUtility;

my $verbose = 1;
my $profile = undef;
my $TarchiveID = undef;
my $query;
my $command;
my $ArchivePerMod;
my $dcmtarpermod_tar; # dcmtar has the format DCM_yy-mm-dd_ImagingUpload-hr-mm-...
my $dcmdirpermod_tar; # dcmdir has the format ImagingUpload-hr-mm-...
my $dcmdirpermod_tar_gz;

my @opt_table = (
    [ "-profile", "string", 1, \$profile,
      "name of config file in ../dicom-archive/.loris_mri"
    ],
    [ "-tarchive_id", "string", 1, \$TarchiveID,
      "tarchive_id of the .tar to be processed from tarchive table"
    ]
); 

my $Help = <<HELP;

This script will parse through the exisiting .tar in the tarchive directory
and regenerate a new .tar that is arranged in subfolders based on series
description 
It can take in tarchiveID as an argument if only a specific .tar is to be 
processed.
HELP

my $Usage = <<USAGE;

Usage: $0 -help to list options

USAGE

&Getopt::Tabular::SetHelp($Help, $Usage);
&Getopt::Tabular::GetOptions(\@opt_table, \@ARGV) || exit 1;

################################################################
################### input option error checking ################
################################################################
{ package Settings; do "$ENV{LORIS_CONFIG}/.loris_mri/$profile" }
if ($profile && !@Settings::db) {
    print "\n\tERROR: You don't have a configuration file named ".
          "'$profile' in:  $ENV{LORIS_CONFIG}/.loris_mri/ \n\n";
    exit 2;
}

################################################################
#### This setting is in a config file (profile)    #############
################################################################
my $tarchiveLibraryDir = $Settings::tarchiveLibraryDir;
$tarchiveLibraryDir    =~ s/\/$//g;

################################################################
######### Establish database connection ########################
################################################################
my $dbh = &NeuroDB::DBI::connect_to_db(@Settings::db);
print "\nSuccessfully connected to database \n";

################################################################
# Grep tarchive list for all those entries with         ########
# NULL in ArchiveLocationPerModality                    ########
################################################################

# Query to grep all tarchive entries
if (!defined($TarchiveID)) {
	$query = "SELECT TarchiveID, ArchiveLocation " .
		 "FROM tarchive ".
	         "WHERE ArchiveLocationPerModality is NULL";
}
# Selecting tarchiveID is redundant here but it makes the while() loop
# applicable to both cases; when a TarchiveID is specified or not
else {
        $query = "SELECT TarchiveID, ArchiveLocation " .
                 "FROM tarchive ".
                 "WHERE TarchiveID = $TarchiveID ";
}

my $sth = $dbh->prepare($query);
$sth->execute();
    
if($sth->rows > 0) {
	# Create tarchive list hash with old and new location
        while ( my $rowhr = $sth->fetchrow_hashref()) {    
		my $TarchiveID = $rowhr->{'TarchiveID'};
        	my $ArchLoc    = $rowhr->{'ArchiveLocation'};
		# Create the temp dir
		my $template = "XXXXXX";
		my $tmpdir = tempdir( $template, TMPDIR => 1, CLEANUP => 1 );
		my $tarchive = $Settings::tarchiveLibraryDir . "/" . $ArchLoc;

		print "Currently processing $tarchive with TarchiveID $TarchiveID \n";
		##### Extract the tarchive
		$ArchivePerMod = &createTarchivePerMod($dbh, $tarchive, $TarchiveID, $tmpdir);

		##### Update database with new ArchiveLocationPerModality
		&insertArchiveLocationPerMod($dbh, $TarchiveID, $ArchivePerMod);

		print "Finished creating per modality tar for TarchiveID $TarchiveID \n";
	}
}
else {
	print "No NULL entries in ArchiveLocationPerModality in tarchive table to be processed \n";	
}

$dbh->disconnect();
exit 0;


=pod
This function will extract the tarchive, and create another one re-arranged
in subfolders based on modality.
Input:  - $dbh = database handler
        - $tarchive = tar name with pull path.
        - $TarchiveID = ID of the tarchive being processed
        - $tmpdir = temporary path where the untar/tar and unzip/zip will take place.
=cut

sub createTarchivePerMod {
	my ($dbh, $tarchive, $TarchiveID, $tmpdir) = @_;
	my @seriesdesc;
	my $searchdir;
	my $dcmdirpermod;

	print "Extracting tarchive $tarchive in $tmpdir/\n" if $verbose;
	$command = "cd $tmpdir ; tar -xf $tarchive";
	`$command`;
	print "Untarring using: $command \n" if $verbose;
	opendir TMPDIR, $tmpdir;
	my @tars = grep { /\.tar\.gz$/ && -f "$tmpdir/$_" } readdir(TMPDIR);
	closedir TMPDIR;

	if(scalar(@tars) != 1) {
		print "Error: Could not find inner tar in $tarchive!\n";
		print @tars . "\n";
		exit(1);
	}
	my $dcmtar = $tars[0];
	my $dcmdir = $dcmtar;
	$dcmdir =~ s/\.tar\.gz$//;
	$dcmdirpermod = $dcmdir . "_permodality";
	my $fullpath_dcmdir = $tmpdir . "/" . $dcmdir;
	my $fullpath_dcmdirpermod = $tmpdir . "/" . $dcmdirpermod;
	my $dcmtarlog = $dcmdir . ".log";
	my $dcmtarmeta = $dcmdir . ".meta";

	# Rename dir to have _permodality suffix, then extract the .tar.gz within
	$command = "cd $tmpdir ; tar -xzf $dcmtar";
	`$command`;
	print "Untar and unzip using: $command \n" if $verbose;
	$command = "mv $fullpath_dcmdir $fullpath_dcmdirpermod";
	`$command`;
	print "Renaming directory using: $command \n" if $verbose;
	$command = "cd $tmpdir ; rm $dcmtar";
	`$command`;
	print "Removing .tar.gz after untarring/unzipping: $command \n" if $verbose;

	# Get all distinct series descriptions, then make directories
	my $query = "SELECT DISTINCT SeriesDescription ".
		    "FROM tarchive_series " .
		    "WHERE TarchiveID = ?";
	my $sth = $dbh->prepare($query);
	$sth->execute($TarchiveID);
    
	while ( my $rowhr = $sth->fetchrow_hashref()) {
        	my $SeriesDesc = $rowhr->{'SeriesDescription'};
		my $SeriesDescTrim = $SeriesDesc;
		$SeriesDescTrim =~ s/\s//g;
        	$command = "mkdir -m 770 " . $fullpath_dcmdirpermod . "/".  $SeriesDescTrim;
        	`$command`;
		print "Making Directories using: $command \n" if $verbose;

		# Get all files for a give series description and move to
		# the newly created subdirectory with the series name
		my $query = "SELECT Filename FROM tarchive_files ".
		    	    "WHERE TarchiveID = ? " .
		            "AND SeriesDescription = ?";
		my $sth = $dbh->prepare($query);
		$sth->execute($TarchiveID,$SeriesDesc);

		while ( my $rowhr = $sth->fetchrow_hashref()) {
     	            my $file = $rowhr->{'Filename'};
	            my $command = "find " . $fullpath_dcmdirpermod . "/" . " -name *" .  $file . "*";  
        	    my $file_found = `$command`;
	    	    chomp ($file_found);
	    	    my $destination_dir = $fullpath_dcmdirpermod . "/" . $SeriesDescTrim . "/";
            	    $command = "mv " . $file_found . " " . $destination_dir;
            	    `$command`;
	            #print "Moving files to destination directory using: $command \n" if $verbose;
       		}
	push @seriesdesc, $SeriesDescTrim;
    	}

	# Remove all directories other than those created based on series description
	# excluding the . and .. directories
	opendir (DIR, $fullpath_dcmdirpermod);
	my @entries = readdir(DIR);
	my @existingdirs = grep {-d "$fullpath_dcmdirpermod/$_" && !(/^\.{1,2}$/)} @entries;

	my %tmp;
	@tmp{@seriesdesc} = ();
	my @diff = grep {! exists $tmp{$_}} @existingdirs;
	print (@diff) if $verbose;


	foreach my $i ( @diff ) {
		$command = "rm -rf " . $fullpath_dcmdirpermod . "/" . $i;
		`$command`;
		print "Removing directory with the command: $command\n" if $verbose;
	}

	# Now create the .tar.gz from directory, emtpy directory from seriesdesc subfolders
	# then move .tar.gz to it, followed by .log and .meta UNchanged
	$dcmdirpermod_tar_gz = $dcmdirpermod . ".tar.gz";
	$command = "cd $tmpdir ; tar -czf $dcmdirpermod_tar_gz $dcmdirpermod";
	`$command`;
	print "Tarring...: $command\n" if $verbose;
	foreach my $i ( @seriesdesc ) {
		$command = "rm -rf " . $fullpath_dcmdirpermod . "/" . $i;
		`$command`;
		print "Removing directory with the command: $command\n";
	}
	$command = "mv " . $tmpdir . "/" . $dcmdirpermod . ".tar.gz " . $fullpath_dcmdirpermod;
	`$command`;
	print "Moving tar using: $command\n" if $verbose;
	$command = "mv " . $tmpdir . "/" . $dcmtarlog . " " . $fullpath_dcmdirpermod;
	`$command`;
	print "Moving log file with the command: $command\n" if $verbose;
	$command = "mv " . $tmpdir . "/" . $dcmtarmeta . " " . $fullpath_dcmdirpermod;
	`$command`;
	print "Moving meta file with the command: $command\n" if $verbose;


	# Get the last subdirectory before the .tar name; this is the year
	# Now create a tar with the new subdirectories architecture
	my @parts = split('/', $tarchive);
	my $year = $parts[@parts-2];
	my $targetloc = $Settings::tarchiveLibraryDir . "/" . $year;
        my ($basenametarpermod, $dir, $ext) = fileparse($tarchive, qr/\..*/);
	$basenametarpermod = $basenametarpermod. "_permodality";
	my $basenametarpermod_tar = $basenametarpermod . ".tar";

	$command = "cd $tmpdir ; tar -cf $targetloc/$basenametarpermod_tar $dcmdirpermod";
	`$command`;
	print "Final Tarring including log and meta files: $command\n" if $verbose;
	$ArchivePerMod = $year . "/" . $basenametarpermod_tar;
	print "Creating per modality tar $basenametarpermod_tar from $fullpath_dcmdirpermod in $targetloc \n" if $verbose;
	return ($ArchivePerMod);
}


=pod
This function will insert the ArchiveLocationPerModality in tarchive.
Input:  - $dbh = database handler
        - $TarchiveID = ID of the tarchive to be updated
	- $ArchivePerMod =  The per modality tar.
=cut

sub insertArchiveLocationPerMod {
    
    my ( $dbh, $TarchiveID, $ArchivePerMod ) = @_;

    my $query = "UPDATE tarchive " .
		"SET ArchiveLocationPerModality = ? " .
		"WHERE TarchiveID = ?";

    my $sth = $dbh->prepare($query);
    $sth->execute($ArchivePerMod, $TarchiveID);
}

