#!/usr/bin/perl
use File::Basename;
use strict;
use Getopt::Long;
use Cwd 'abs_path';

sub usage{
	print <<USAGE;
usage:
		perl $0 [options]
author
		zhouxianqiang  zhouxianqiang\@genomics.cn
options:
		-help   : print help info
	-P: used to qsub -P(defualt:CNGDagi)
	-project: project code
	-reqsub : y,yes,Y,YES or logFile (your jobs will do used you before result)
	-see	: only print the shell
	-quene	: default:bc.q
	-rate	: source=source*rate (default:1)
	-Main	: qsub|qdel|stat (default:qsub)
	-ReqsubNum	:reqsub num
	-minStorageSize	:disk is ranged to the min Memory Size will qmod (default:200G)
e.g.:
	perl $0 All_dependence.txt -P BACnhhM -project F15FTSSCKF3238_BAChioM
USAGE
}


my (%AllShell,%ShellJobid,%JobidShell,%ErrorTime,%parent,%ShellSource,%CodeProcessID,$help,$reqsub,$Project,$P,$see,$quene,$Rate,$Main,$Error_num,$MinStorageSize,%CheckPath);
GetOptions(
	"help"=>\$help,
	"reqsub=s"=>\$reqsub,
	"project=s"=>\$Project,
	"P=s"=>\$P,
	"see=s"=>\$see,
	"quene=s"=>\$quene,
	"rate=s"=>\$Rate,
	"Main=s"=>\$Main,
	"ReqsubNum=s"=>\$Error_num,
	"minStorageSize=s"=>\$MinStorageSize,
);

my $StartTime=`date "+%Y-%m-%d,%H:%M:%S"`;
my $CWD=abs_path("./");
my $UpdateFile="$CWD/monitor$$.stat";
my $QMODFLAG = 0;
my $DIEDFLAG = 0;
$Main||="qsub";
$Rate||=1;
$Error_num||=5;
$P||="CNGDdx2";
$quene||="bc.q";
$MinStorageSize||="200G";

$MinStorageSize = unit_conversion($MinStorageSize);

my $user=`whoami`;
chomp($user);

if(defined $help or !defined $Project or (!defined $ARGV[0] and $Main eq "qsub")){
	if($Main eq "qdel" or $Main eq "stat"){
	}else{
		&usage();
		exit 0;
	}
}

read_conf($ARGV[0],\%parent,\%ShellSource,\%AllShell) if(defined $ARGV[0]);
read_monitor_conf(\%CodeProcessID);

if($Main eq "qsub"){
	if(exists $CodeProcessID{$Project} and $CodeProcessID{$Project}->[1] eq "Doing"){
		if(findBackGroundJobID($Project, $CodeProcessID{$Project}->[0]) == 1){
					print "there is same project is doing(ProcessID:$CodeProcessID{$Project}->[0]), please check, before do again must kill this Process\nCommand:     perl $0 -Main qdel -project $Project\n";
					exit(0);
		}
	}
	open LOG,">monitor$$.log" or die $!;
	open INFO,">monitor$$.info" or die $!;
	if($reqsub eq "Y" or $reqsub eq "y" or $reqsub=~/^yes$/i or (defined $reqsub && !-e $reqsub)){
		$reqsub=$CodeProcessID{$Project}->[3];
	}
	do_requb(\%AllShell,\%JobidShell,\%ShellJobid,$reqsub) if(defined $reqsub);
	$CodeProcessID{$Project}=[$$,"Doing","$CWD/monitor$$.stat","$CWD/monitor$$.log"];
	write_conf(\%CodeProcessID);
	Main_Run(\%AllShell,\%ShellJobid,\%JobidShell,\%ErrorTime,\%ShellSource,$Error_num,\%parent,$P,$quene);

}elsif($Main eq "qdel"){
	$CodeProcessID{$Project}->[1] = "Qdel";
	write_conf(\%CodeProcessID);
	if(findBackGroundJobID($Project, $CodeProcessID{$Project}->[0]) == 1){
		print "Your Project:$Project is Killing at anthor thread!\nplease Wait...\n";
		while(1){
			sleep 5;
			if(findBackGroundJobID($Project, $CodeProcessID{$Project}->[0]) == 0){
				print "Qdel DoneSucceed\n";
				last;
			}
		}
	}else{
		print "$Project:Monitor is Done, Qdel job by log File\n";
		doQdel();
	}
}elsif($Main eq "stat"){
	my %jobProcessID;
	foreach my$Code(keys %CodeProcessID){
		if(findBackGroundJobID($Code, $CodeProcessID{$Code}->[0]) == 1){
			$jobProcessID{$CodeProcessID{$Code}->[0]} = ["BackgroundMonitor",$Code];
		}else{
			$jobProcessID{$CodeProcessID{$Code}->[0]} = ["DoneSucceed",$Code];
		}
	}
	print "#Project\tTotal\tDone\tDoing\tFalse\tUndo\tMonitorStat\n";
	my @keys=(defined $Project) ? ($Project):keys %CodeProcessID;
	foreach my$Code(sort @keys){
		next if($CodeProcessID{$Code}->[1] eq "Qqel");
		if(-e "$CodeProcessID{$Code}->[2]"){
			open STAT,"$CodeProcessID{$Code}->[2]" or die $!;
			my $total=0;
			my %tmp=("Done"=>0,"Doing"=>0,"False"=>0,"Undo"=>0);
			while(<STAT>){
				chomp;
				my $stat=(split/\t/)[-1];
				$tmp{$stat}++;
				$total++;
			}
			close STAT;
			my $ProcessID=$CodeProcessID{$Code}->[0];
			if(($tmp{False}>0 or $tmp{Undo}>0) and $jobProcessID{$ProcessID}->[0] eq "DoneSucceed"){
				$jobProcessID{$ProcessID}->[0]="ExitError,please_check_and_do_again";
			}
			print "$Code\t$total\t$tmp{Done}\t$tmp{Doing}\t$tmp{False}\t$tmp{Undo}\t$jobProcessID{$ProcessID}->[0]\n";
		}else{
			print "$Code can't find statFile $CodeProcessID{$Code}->[2]\n";
		}
	}
}else{
	print "Unkown Main project\n";
}


sub doQdel(){

	my $id=$CodeProcessID{$Project}->[0];
	my $file=$CodeProcessID{$Project}->[2];
	my $log=$CodeProcessID{$Project}->[3];
	if(-e "$log"){
	my %tmp;
	readQsubLog($log,\%tmp);
	my $return=testCMD("qstat",10,1);
	if($return eq "0"){
		print INFO "network communication error,please try again,exit\n";
		exit(0);
	}
	my @qstat=split/\n/,$return;
	foreach my$key(keys %tmp){
		my $jobid=$tmp{$key}->[2];
		if($key=~/\tQsub/){
			foreach(@qstat){
				if($_=~/^$jobid\s/){
					my $return=testCMD("qdel $jobid",2);
					if($return eq "0"){
						print INFO "qdel $jobid error,please check\n";
					}
				}
			}
		}
	}
	}else{
		print "No find log file, Qdel Error\nPlease check computational node jobs stat\n";
	}
	$CodeProcessID{$Project}=[$id,"Qqel",$file,$log];
	write_conf(\%CodeProcessID);
}


sub Main_Run(){
	my $AllShell=shift;
	my $ShellJobid=shift;
	my $JobidShell=shift;
	my $ErrorTime=shift;
	my $ShellSource=shift;
	my $Error_num=shift;
	my $parent=shift;
	my $P=shift;
	my $quene=shift;
	while(1){
		my @CanDo;
		my %tmp_code_process;
		my $time = `date "+%Y-%m-%d,%H:%M:%S"`;

		## check qdel sign
		read_monitor_conf(\%tmp_code_process) if(-e "/home/$user/.monitor.conf");
		if($tmp_code_process{$Project}->[1] eq "Qdel"){
			print "Project:$Project get an Qdel sign , Qdeling Now.\n please Wait...\n";
			judge($JobidShell,$ShellJobid,$ErrorTime,$AllShell);
			foreach my$jobid(keys %$JobidShell){
				testCMD("qdel $jobid",2);
			}
			update($AllShell);
			print INFO "get an Qdel sign , exit at $time";
			print "Qdel Done\n";
			exit(0);
		}

		## get died node
		my %diedHash;
		if($DIEDFLAG==10){
			getDiedNode(\%diedHash);
			$DIEDFLAG=0;
		}
		$DIEDFLAG++;
		## throw cando job
		my($DoneNum,$DoingNum,$FalseNum,$UndoNum)=update($AllShell,$ShellJobid);
		find_cando_shell($AllShell,$ShellJobid,$ErrorTime,$Error_num,$parent,\@CanDo);
		qsub_shell(\@CanDo,$JobidShell,$ShellJobid,$ShellSource,$P,$quene);
		judge($JobidShell,$ShellJobid,$ErrorTime,$AllShell,\%diedHash);
		## check disk
		foreach my $tmpPath(keys %CheckPath){
			checkDiskStat($tmpPath, $JobidShell);
		}

		## check all job stat
		if($DoingNum==0 and $FalseNum==0 and $UndoNum==0){
			print INFO "All Job done Correctly at $time\n";
			return 0;
		}else{
			if($FalseNum>0 and $DoingNum==0 and @CanDo==0){
				print INFO "$FalseNum num Job Error,please check,exit at $time\n";
				exit(0);
			}
		}
		sleep 10;
	}

}

sub do_requb(){
	my $AllShell=shift;
	my $JobidShell=shift;
	my $ShellJobid=shift;
	my $log=shift;
	if(-e $log){
		my %tmp;
		if(readQsubLog($log,\%tmp)==0){
			close INFO;
			close LOG;
			`rm monitor$$.log monitor$$.info`;
			exit(0);
		}
		my $return=testCMD("qstat",10,1);
		if($return eq "0"){
			print INFO "network communication error,please try again,exit\n";
			exit(0);
		}
		my @job=split/\n/,$return;
		foreach my$sh(keys %$AllShell){
			if(exists $tmp{"$sh\tDone"}){
				$AllShell->{$sh}=1;
				print LOG join("\t",@{$tmp{"$sh\tDone"}})."\n";
			}elsif(exists $tmp{"$sh\tQsub"}){
				my $find=0;
				foreach my$job(@job){
					my $jobid=$tmp{"$sh\tQsub"}->[2];
					if($job=~/^\s*$jobid\s+/){
						print LOG join("\t",@{$tmp{"$sh\tQsub"}})."\n";
						$JobidShell->{$jobid}=$sh;
						$ShellJobid->{$sh}=$jobid;
						$find=1;
					}
				}
				my $taskStat = tastDoneStat("$sh.sign","Still_waters_run_deep",2,1,1);
				if($find==0 and $taskStat == 1){
					$AllShell->{$sh}=1;
#					print LOG join("\t",@{$tmp{"$sh\tQsub"}})."\n";
					$tmp{"$sh\tQsub"}->[0]="Done";
					print LOG join("\t",@{$tmp{"$sh\tQsub"}})."\n";
				}
			}
		}
		update($AllShell,$ShellJobid);
	}else{
		foreach my$sh(keys %$AllShell){
			my $taskStat = tastDoneStat("$sh.sign","Still_waters_run_deep",2,1,1);
			if($taskStat == 1){
				print LOG "Done\t$sh\t0000000\t$StartTime";
#				print LOG "Done\t$sh\t0000000\tbc.q\@compute0000\t$StartTime\n";
				$AllShell->{$sh}=1;
			}else{

			}
		}
	}
}

sub read_monitor_conf(){
	my $CodeProcessID=shift;
	open CONF,"/home/$user/.monitor.conf" or die $!;
	while(<CONF>){
		chomp;
		my($Code,$ProcessID,$stat,$file,$log)=split/\t/;
		$CodeProcessID->{$Code}=[$ProcessID,$stat,$file,$log];
	}
	close CONF;
}

sub write_conf(){
	my $CodeProcessID=shift;
	open CONF,">/home/$user/.monitor.conf";
	foreach(keys %$CodeProcessID){
		print CONF "$_\t".join("\t",@{$CodeProcessID->{$_}})."\n";
	}
	close CONF;
}

sub update(){
	my $AllShell=shift;
#	my $file=shift;
	my $ShellJobid=shift;
	my $FalseNum=0;
	my $DoneNum=0;
	my $DoingNum=0;
	my $UndoNum=0;
	open F,">$UpdateFile";
	foreach(keys %$AllShell){
		if($AllShell->{$_}==1){
			$DoneNum++;
			print F "$_\tDone\n";
		}elsif(defined $ShellJobid and exists $ShellJobid->{$_}){
			$DoingNum++;
			print F "$_\tDoing\n";
		}elsif($AllShell->{$_}==0){
			$FalseNum++;
			print F "$_\tFalse\n";
		}else{
			$UndoNum++;
			print F "$_\tUndo\n";
		}
	}
	close F;
	return ($DoneNum,$DoingNum,$FalseNum,$UndoNum);
}


sub judge(){
	my $JobidShell=shift;
	my $ShellJobid=shift;
	my $ErrorTime=shift;
	my $AllShell=shift;
	my $diedHash = shift;
	my $return=testCMD("qstat",10,1);
	if($return eq "0"){
		print INFO "network communication error,judge pass\n";
		return 0;
	}
	my @qstat=split/\n/,$return;
	my %tmp;
	foreach(@qstat){
		$tmp{$1}="" if($_=~/^\s*(\d+)/);
		my $jobID = $1;
		## deal Running at Died Node
		if(exists $JobidShell->{$jobID}){
			my $line = $_;
			foreach my$cpu(keys %$diedHash){
				if($line=~/[\-\.]$cpu[\-\.]/){
					testCMD("qdel $jobID",2);
					my $Time=`date "+%Y-%m-%d,%H:%M:%S"`;
					print LOG "Qdel-RunAtDiedNode\t$JobidShell->{$jobID}\t$jobID\t$Time";
					my $shell = $JobidShell->{$jobID};
					delete $JobidShell->{$jobID};
					delete $ShellJobid->{$shell};
					last;
				}
			}
		}
	}
	foreach my$jobid(keys %$JobidShell){
		my $find=0;
		my $shell=$JobidShell->{$jobid};
		if(!exists $tmp{$jobid}){
			my $Time=`date "+%Y-%m-%d,%H:%M:%S"`;
			my $stat = tastDoneStat("$shell.sign","Still_waters_run_deep",5,1);
			if($stat == 1){
				$AllShell->{$shell}=1;
				print LOG "Done\t$shell\t$jobid\t$Time";
				delete $JobidShell->{$jobid};
				delete $ShellJobid->{$shell};
			}elsif($stat == -1){
				$ErrorTime->{$shell}++;
				print LOG "Error\t$shell\t$jobid\t$Time";
				if(-e "$shell.o$jobid" && -e "$shell.e$jobid"){
					`mv $shell.o$jobid $shell.o.before && mv $shell.e$jobid $shell.e.before`;
					print INFO "please check $shell.e.before\n";
					$AllShell->{$shell}=0;
					delete $JobidShell->{$jobid};
					delete $ShellJobid->{$shell};
				}	
			}

		}
	}
}


sub tastDoneStat(){
	my $signFile = shift;
	my $signFlag = shift;
	my $tryTimes = shift;
	my $sleepTime = shift;
	my $flag = shift;
	# return: 1. means done succeed  -1. means tast error  0. means disk connected error
	for(0..$tryTimes){
		if(-e $signFile){
			if(`cat $signFile`=~/$signFlag/m){
				return 1;
			}
		}
		sleep $sleepTime;
	}
	return -1 if(defined $flag);
	if($? == 0){
		return -1;
	}else{
		return 0;
	}
}


sub qsub_shell(){
	my $array=shift;
	my $JobidShell=shift;
	my $ShellJobid=shift;
	my $ShellSource=shift;
	my $P=shift;
	my $quene=shift;
	foreach my$sh(@$array){
		my $source = $Rate*$ShellSource->{$sh};
		my($shell_name,$qsub_dir)=fileparse($sh);
		chdir($qsub_dir);
		my $cmd="qsub -cwd -P $P -q $quene -l vf=${source}G -l num_proc=1 $sh";
		`rm $sh.sign` if(-e "$sh.sign");
		my $job_return=testCMD($cmd,5);
		if($job_return eq "0"){
			print "$P or $quene or network communication may error,exit\n";
			exit(0);
		}
		if($job_return =~ /Your job (\d+)/){
			$JobidShell->{$1}=$sh;
			$ShellJobid->{$sh}=$1;
			my $Time=`date "+%Y-%m-%d,%H:%M:%S"`;
			print LOG "Qsub\t$sh\t$1\t$Time";
		}elsif($job_return=~/does not exist/){
			print "$Project does not exist!!!!\nexit\n";
			exit(0);
		}
	}
}


sub find_cando_shell(){
	my $AllShell=shift;
	my $ShellJobid=shift;
	my $ErrorTime=shift;
	my $Error_num=shift;
	my $parent=shift;
	my $array=shift;
	foreach my$sh(keys %$AllShell){
		next if($AllShell->{$sh}==1 or exists $ShellJobid->{$sh} or $ErrorTime->{$sh}>=$Error_num);
		if(!$parent->{$sh}){
			push @$array,$sh;
		}else{
			my $count=0;
			foreach my$tmp(@{$parent->{$sh}}){
				$count++ if($AllShell->{$tmp}==1);
			}
			push @$array,$sh if($count==@{$parent->{$sh}});
		}
	}
}





sub read_conf(){
	my $file=shift;
#	my $child=shift;
	my $parent=shift;
	my $ShellSource=shift;
	my $AllShell=shift;
	open F,$file or die $!;
	while(<F>){
		chomp;
		my @tmp=split/\s+/,$_;
		if(@tmp==1){
			my($sh,$source)=split/:/,$tmp[0];
			my $num=unit_conversion($source);
			$ShellSource->{$sh}=$num;
			$AllShell->{$sh}=-1;
			$CheckPath{$1}="" if($sh=~/^(\/[^\/]+\/[^\/]+)/);
		}elsif(@tmp==2){
			my($sh,$source)=split/:/,$tmp[0];
			my($sh1,$source1)=split/:/,$tmp[1];
			my $num=unit_conversion($source);
			my $num1=unit_conversion($source1);
			$ShellSource->{$sh}=$num;
			$ShellSource->{$sh1}=$num1;
#			push @{$child->{$sh}},$sh1;
			push @{$parent->{$sh1}},$sh;
			$AllShell->{$sh}=-1;
			$AllShell->{$sh1}=-1;
			$CheckPath{$1}="" if($sh=~/^(\/[^\/]+\/[^\/]+)/);
			$CheckPath{$1}="" if($sh1=~/^(\/[^\/]+\/[^\/]+)/);
		}else{
			print "Conf File format erro!!\n";
			exit(0);
		}
	}
	close F;
}






sub readQsubLog(){
	my $file=shift;
	my $hash=shift;
	open F,$file or die $!;	
	while(<F>){
		next if($_=~/^[#\s]/);
		chomp;
		my @tmp=split/\t/,$_;
		if(@tmp!=4){
			print "$_\n$file  Format error!!!\nplease check and try again\n";
			return 0;
		}
#		my($stat,$sh,$JobID,$time)=split/\t/,$_;
		$hash->{"$tmp[1]\t$tmp[0]"}=\@tmp;
	}
	close F;
	return 1;
}


sub checkDiskStat(){
	my $path=shift;
	my $JobidShell=shift;
	my $return = testCMD("df -h $path", 2);
	if($return ne 0){
		my $lastLine = (split/\n/,$return)[-1];
		if($lastLine =~/\S+\s+\S+\s+(\S+)\s+\S+\%/){
			my $storage = $1;
			my $size = unit_conversion($storage);
			if($size < $MinStorageSize){
				jobQmod($JobidShell);
				print INFO "JobQmod\tSystemInfo:$lastLine\n" if($QMODFLAG==0);
				$QMODFLAG = 1;
			}else{
				jobUnQmod($JobidShell);
				print INFO "JobUnQmod\tSystemInfo:$lastLine\n" if($QMODFLAG==1);
				$QMODFLAG = 0;
			}
		}
	}
}

sub jobUnQmod(){
	my $JobidShell=shift;
	foreach my$jobID (keys %$JobidShell){
		testCMD("qmod -us $jobID",2);
	}
}

sub jobQmod(){
	my $JobidShell=shift;
	foreach my$jobID (keys %$JobidShell){
		testCMD("qmod -s $jobID",2);
	}
}

sub getDiedNode(){
	my $diedHash = shift;
	my $return = testCMD("qhost", 2);
	if($return ne 0){
		my @lines = split /\n/,$return;
		shift @lines for (1 .. 3); ##remove the first three title lines

		foreach  (@lines) {
			my @t = split /\s+/;
			my $node_name = $t[0];
			$diedHash->{$node_name} = 1 if($t[3]=~/-/ || $t[4]=~/-/ || $t[5]=~/-/ || $t[6]=~/-/ || $t[7]=~/-/);
		}
	}
}

sub findBackGroundJobID(){
	my $patton = shift;
	my $jobID = shift;
	my $return = testCMD("ps -ef | grep $patton", 5);
	if($return ne 0){
		foreach(split/\n/,$return){
			if($_=~/^\S+\s+(\d+)/){
				if($1 eq $CodeProcessID{$patton}->[0]){
					return 1;
				}
			}
		}

	}
	return 0;
}


sub testCMD(){
	my $cmd=shift;
	my $testNum=shift;
	my $sleep=shift;
	if(!defined $sleep){
		$sleep=0.5;
	}
	my $cmdReturn=`$cmd`;
	while($?!=0){
		if($testNum==0){
			return "0";
		}
		$cmdReturn=`$cmd`;
		sleep $sleep;
		$testNum--;
	}
	return $cmdReturn;
}


sub unit_conversion(){
	my $data=shift;
	if($data=~/[mtg]/i){
		if($data=~/m/i){
			$data=~s/m//i;
			return $data/1000;
		}elsif($data=~/t/i){
			$data=~s/t//i;
			return $data*1000;
		}elsif($data=~/g/i){
			$data=~s/g//i;
			return $data;
		}
	}else{
		if($data=~/[a-z]/i){
			print "erro unit\n";
			exit(0);
		}
		return $data;
	}
}

