    Dim facetsrptConnectionString As New String("")
    Dim facetsrptSqlConnection As New System.Data.SqlClient.SqlConnection
    Dim userId As String = ""
    Dim adGroup As String = ""
    Dim Application As Object

    'This method is called when this script task executes in the control flow.
    'Before returning from this method, set the value of Dts.TaskResult to indicate success or failure.
    'To open Help, press F1.

    Public Sub Main()
        Try
            openFacetrptDB()
        Catch ex As Exception
            MessageBox.Show(ex.Message, " Error", MessageBoxButtons.OK, MessageBoxIcon.Error) 'This will give a description of the error.
            Exit Sub
        End Try

        truncateADTable()

        Dim searcher As New DirectorySearcher("")
        Try
            ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            '' LDAP Search Query
            '' sAMAccountType=805306368 - This is the value for a normal user object
            ''                          - Used in the search to exclude group and domain object accounts
            '' userAccountControl:1.2.840.113556.1.4.803:=65536 - userAccountControl (UAC) property to indicate a non-expiring password
            ''                                                  - Used in the search in a "not" statement to exclude service accounts
            ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            searcher.Filter = "(&(sAMAccountType=805306368) (!(userAccountControl:1.2.840.113556.1.4.803:=65536)))"
            searcher.SearchScope = SearchScope.Subtree
            searcher.PageSize = 1000

            Dim UserName As String

            For Each result As SearchResult In searcher.FindAll()
                UserName = ""

                If Not (IsNothing(result)) Then
                    UserName = result.Properties("samaccountname")(0)
                    userId = UserName
                End If
                'MsgBox("User Name : " + UserName + "", MsgBoxStyle.Information + MsgBoxStyle.OkOnly, "Active Directory User Information And Its Group(s) in VB. NET")
                If Not (IsNothing(UserName)) And UserName <> "" Then
                    Call GetActiveDirectoryUserGroups(UserName)
                End If
            Next

        Catch ex As Exception
            'MessageBox.Show(ex.Message, " Error", MessageBoxButtons.OK, MessageBoxIcon.Error) 'This will give a description of the error.
            Exit Sub
        Finally
            'MsgBox("Active directory (LDAP) user details and their belonging group(s) information has been exported successfully in application path on ..bin/ADSIUsersAndTheirGroupsList.txt file.", MsgBoxStyle.Information + MsgBoxStyle.OKOnly, "Active Directory User Information And Its Group(s) in VB. NET")
            searcher.Dispose()
        End Try

        facetsrptSqlConnection.Close()
        Dts.TaskResult = ScriptResults.Success
    End Sub

    Public Sub GetActiveDirectoryUserGroups(ByVal UserName As String)

        Dim search As New DirectorySearcher("")
        Dim groupCount As Int64
        Dim counter As Int64
        Dim GroupName As String
        Dim GroupArr As Array
        Dim DN As String
        Try
            ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            '' LDAP Search Query
            '' sAMAccountType=805306368 - This is the value for a normal user object
            ''                          - Used in the search to exclude group and domain object accounts
            '' userAccountControl:1.2.840.113556.1.4.803:=65536 - userAccountControl (UAC) property to indicate a non-expiring password
            ''                                                  - Used in the search in a "not" statement to exclude service accounts
            '' samaccountname - Further restricts the search to a specific username
            ''                - Included to allow us to find a specific username and iterate through its MemberOf list to get groups
            ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            search.Filter = "(&(sAMAccountType=805306368) (!(userAccountControl:1.2.840.113556.1.4.803:=65536)) (samaccountname=" + UserName.ToString.Trim + "))"
            search.PropertiesToLoad.Add("distinguishedName")
            search.PropertiesToLoad.Add("MemberOf")

            Dim result As SearchResult = search.FindOne()

            If Not (IsNothing(result)) Then

                Try
                    groupCount = result.Properties("MemberOf").Count
                Catch ex As NullReferenceException
                    groupCount = 0
                End Try

                DN = ""
                DN = CStr(result.Properties("distinguishedName")(0))

                If groupCount > 0 Then
                    For counter = 0 To groupCount - 1
                        GroupName = ""
                        GroupName = CStr(result.Properties("MemberOf")(counter))
                        GroupArr = Split(GroupName, ",")
                        '' GroupArr(0) indicates name component of group's DN
                        If Not (IsNothing(GroupArr(0))) Then
                            adGroup = Mid(GroupArr(0), 4, Len(GroupArr(0)) - 3)
                            '' Only writing groups to SQL table that follow the form of <PART1>-<environment>-<PART2>
                            '' This includes the SQL Project open item on <PART3> permissions, which will be the same group name as above but with -<PART3> appended
                            ''If adGroup.Contains("<PART1>") And adGroup.Contains("<PART2>") Then
                            insertIntoADTable(userId, adGroup)
                            'MsgBox("Group(s) belongs to " + UserName + " - " + GroupName + " ", MsgBoxStyle.Information + MsgBoxStyle.OkOnly, "Active Directory User Information And Its Group(s) in VB. NET")
                            ''End If
                        End If
                'adGroup = GroupName
                    Next
                Else
                    '' MemberOf list is empty, therefore no associated groups outside of the Primary Group of Domain User, which we don't care about
                    '' Instead, parse full DN of the user only if it is in the Disabled Users OU, and write the OU to the table.
                    '' This will allow us to group the Disabled Accounts separately in the report
                    '' GroupArr(1) indicates the OU component of the user's DN
                    If DN.Contains("OU=Disabled Users") Then
                        GroupArr = Split(DN, ",")
                        If Not (IsNothing(GroupArr(1))) Then
                            adGroup = Mid(GroupArr(1), 4, Len(GroupArr(1)) - 3)
                            insertIntoADTable(userId, adGroup)
                        End If
                    End If
                End If
            End If
        Catch ex As Exception
            'MsgBox(ex.Message, MsgBoxStyle.Information + MsgBoxStyle.OkOnly, "Active Directory User Information And Its Group(s) in VB. NET")
            Exit Sub
        Finally
            search.Dispose()
        End Try
    End Sub

    Private Sub openFacetrptDB()
        If Dts.Variables("server").Value.ToString.ToLower() = "val1" Then
            facetsrptConnectionString = "Server=<VAL1Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val2" Then
            facetsrptConnectionString = "<VAL2Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val3" Then
            facetsrptConnectionString = "<VAL3Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val4" Then
            facetsrptConnectionString = "<VAL4Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val5" Then
            facetsrptConnectionString = "<VAL5Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val6" Then
            facetsrptConnectionString = "<VAL6Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val7" Then
            facetsrptConnectionString = "<VAL7Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val8" Then
            facetsrptConnectionString = "<VAL8Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        ElseIf Dts.Variables("server").Value.ToString.ToLower() = "val9" Then
            facetsrptConnectionString = "<VAL9Server>;Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        Else
            facetsrptConnectionString = "Server=<DEFAULTServer>" + Dts.Variables("server").Value + ";Database=<db>;User Id=" + Dts.Variables("user").Value + ";Password=" + Dts.Variables("pass").Value
        End If
        facetsrptSqlConnection.ConnectionString = facetsrptConnectionString
        facetsrptSqlConnection.Open()
    End Sub

    Private Sub truncateADTable()
        Dim cmd As New System.Data.SqlClient.SqlCommand
        cmd.CommandType = System.Data.CommandType.Text
        cmd.CommandText = "DELETE FROM <AD TABLE>"
        cmd.Connection = facetsrptSqlConnection

        cmd.ExecuteNonQuery()
    End Sub

    Private Sub insertIntoADTable(USUS_ID As String, AD_GROUP As String)
        Dim cmd As New System.Data.SqlClient.SqlCommand
        cmd.CommandType = System.Data.CommandType.Text
        cmd.CommandText = "INSERT INTO <AD TABLE> (USUS_ID, AD_GROUP) VALUES ('" + USUS_ID + "', '" + AD_GROUP + "')"
        cmd.Connection = facetsrptSqlConnection

        cmd.ExecuteNonQuery()
    End Sub
