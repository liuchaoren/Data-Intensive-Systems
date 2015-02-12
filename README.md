# cs516-team
Team repository for CS516

Installation
------------
1. Install [VirtualBox](https://www.virtualbox.org/).
2. Install [Vagrant](http://vagrantup.com).
3. Download the Vagrantfile from this repository and place it in the folder you want your VM to reside.
4. Open up a command prompt, go to that folder, and type "vagrant up". 
5. Once that is done, type "vagrant ssh" to get into the virtual machine.
6. Check out the project repository into your virtual machine: "git clone https://github.com/bwalenz/cs516-team.git"

Notes
-----
The test data contains a votes.csv file detailing two different congressional representatives voting
records through the years 2011-2012. The file is formatted as:

person_a_vote, person_b_vote, vote_date, bill_number, year, chamber(h or s), status

 
