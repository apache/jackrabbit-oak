pluginZip=`ls /tmp/plugins | grep elastiknn-7.10 | head -n 1`
echo "Installing plugin /tmp/plugins/$pluginZip"
bin/elasticsearch-plugin install --batch file:///tmp/plugins/$pluginZip
su -c "bin/elasticsearch" elasticsearch